#include <optional>
#include <vector>
#include <span>
#include <unordered_map>
#include <algorithm>

#include "Parameters.hpp"

// Encode a uint32_t as a variable-length integer (varint)
// Returns the number of bytes written
inline size_t encode_varint(uint32_t value, std::byte* output) {
    size_t bytes = 0;
    while (value >= 128) {
        output[bytes++] = static_cast<std::byte>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    output[bytes++] = static_cast<std::byte>(value & 0x7F);
    return bytes;
}

// Decode a varint from input, updating offset to the next byte position
inline uint32_t decode_varint(const std::byte* input, size_t& offset) {
    uint32_t value = 0;
    int shift = 0;
    while (true) {
        std::byte b = input[offset++];
        value |= (static_cast<uint32_t>(b) & 0x7F) << shift;
        if ((static_cast<uint32_t>(b) & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

// Build a compressed frequency index from input data
// Index format: [num_distinct(4B)][counts_offset(4B)][delta-varint values][bitmap][varint counts]
// Returns empty vector if index is not cost-effective
std::vector<std::byte> build_idx(std::span<const uint32_t> data, Parameters config){
    // Count frequency of each distinct value
    std::unordered_map<uint32_t, uint32_t> freq_map;
    for (const auto val : data) {
        freq_map[val]++;
    }

    const size_t num_distinct = freq_map.size();
    const double cost_ratio = static_cast<double>(config.f_a) / static_cast<double>(config.f_s);

    constexpr double estimated_queries_per_block = 550.0;

    // Sort values for delta encoding
    std::vector<std::pair<uint32_t, uint32_t>> items;
    items.reserve(num_distinct);
    for (const auto& [value, count] : freq_map) {
        items.emplace_back(value, count);
    }
    std::ranges::sort(items,
                      [](const auto& a, const auto& b) { return a.first < b.first; });
    
    std::vector<std::byte> index;
    index.reserve(8 + num_distinct * 3);

    // Header: [num_distinct(4B)][counts_offset(4B) - filled later]
    const auto num_distinct_u32 = static_cast<uint32_t>(num_distinct);
    index.resize(8);
    std::memcpy(index.data(), &num_distinct_u32, sizeof(uint32_t));
    size_t offset = 8;
    
    // Encode values section using delta encoding with varint compression
    // First value is stored directly, subsequent values store delta from previous
    uint32_t prev_value = 0;
    for (size_t i = 0; i < items.size(); i++) {
        uint32_t value = items[i].first;
        
        if (i == 0) {
            std::byte varint_buf[5];
            size_t varint_len = encode_varint(value, varint_buf);
            index.resize(offset + varint_len);
            std::memcpy(index.data() + offset, varint_buf, varint_len);
            offset += varint_len;
        } else {
            uint32_t delta = value - prev_value;
            std::byte varint_buf[5];
            size_t varint_len = encode_varint(delta, varint_buf);
            index.resize(offset + varint_len);
            std::memcpy(index.data() + offset, varint_buf, varint_len);
            offset += varint_len;
        }
        prev_value = value;
    }
    
    // Encode counts section using bitmap + varint compression
    // Bitmap: 1 bit per value (1 = count is 1, 0 = count stored in varint section)
    // Varint section: only stores counts >= 2 as (count - 2)
    const auto counts_offset = static_cast<uint32_t>(offset);
    std::memcpy(index.data() + 4, &counts_offset, sizeof(uint32_t));
    
    // Allocate and initialize bitmap
    size_t bitmap_bytes = (num_distinct + 7) / 8;
    index.resize(offset + bitmap_bytes);
    std::memset(index.data() + offset, 0, bitmap_bytes);
    size_t bitmap_offset = offset;
    offset += bitmap_bytes;
    
    // Encode counts: count=1 sets bitmap bit, count>=2 stores (count-2) as varint
    for (size_t i = 0; i < items.size(); i++) {
        if (const uint32_t count = items[i].second; count == 1) {
            size_t byte_idx = i / 8;
            const size_t bit_idx = i % 8;
            index[bitmap_offset + byte_idx] = static_cast<std::byte>(
                static_cast<uint32_t>(index[bitmap_offset + byte_idx]) | (1 << bit_idx)
            );
        } else {
            std::byte varint_buf[5];
            const size_t varint_len = encode_varint(count - 2, varint_buf);
            index.resize(offset + varint_len);
            std::memcpy(index.data() + offset, varint_buf, varint_len);
            offset += varint_len;
        }
    }

    // Cost-benefit analysis: skip index if it's not cost-effective
    const auto actual_size_bytes = static_cast<double>(index.size());

    if (const double break_even_bytes = estimated_queries_per_block * cost_ratio * 1024.0; actual_size_bytes > break_even_bytes) {
        return {};  // Index too large, not worth the storage cost
    }
    
    return index;
}

// Query the frequency of a value in the index
// Returns std::nullopt if no index, 0 if value not found, otherwise the count
std::optional<size_t> query_idx(uint32_t predicate, const std::vector<std::byte>& index){
    if (index.empty()) {
        return std::nullopt;
    }
    
    // Read header
    uint32_t num_indexed;
    uint32_t counts_offset;
    std::memcpy(&num_indexed, index.data(), sizeof(uint32_t));
    std::memcpy(&counts_offset, index.data() + 4, sizeof(uint32_t));
    
    // Decode delta-encoded values to find the predicate
    size_t value_offset = 8;
    uint32_t current_value = 0;
    int found_idx = -1;
    
    for (uint32_t i = 0; i < num_indexed; i++) {
        if (i == 0) {
            current_value = decode_varint(index.data(), value_offset);
        } else {
            uint32_t delta = decode_varint(index.data(), value_offset);
            current_value += delta;
        }
        
        if (current_value == predicate) {
            found_idx = i;
            break;
        }
        
        // Values are sorted, so we can early exit
        if (current_value > predicate) {
            return 0;
        }
    }
    
    if (found_idx < 0) {
        return 0;
    }

    // Decode the count for the found value
    const size_t bitmap_bytes = (num_indexed + 7) / 8;
    const size_t bitmap_offset = counts_offset;
    size_t varint_offset = counts_offset + bitmap_bytes;
    
    size_t byte_idx = found_idx / 8;
    size_t bit_idx = found_idx % 8;

    // Check bitmap: if bit is set, count is 1
    bool is_one = (static_cast<uint32_t>(index[bitmap_offset + byte_idx]) & (1 << bit_idx)) != 0;
    if (is_one) {
        return 1;
    }
    
    // Count how many non-one entries appear before this index
    // to find the correct position in the varint section
    size_t non_ones_before = 0;
    for (int i = 0; i < found_idx; i++) {
        byte_idx = i / 8;
        bit_idx = i % 8;
        bool is_one_i = (static_cast<uint32_t>(index[bitmap_offset + byte_idx]) & (1 << bit_idx)) != 0;
        if (!is_one_i) {
            non_ones_before++;
        }
    }
    
    // Skip over the varints before our target
    for (size_t i = 0; i < non_ones_before; i++) {
        while ((static_cast<uint32_t>(index[varint_offset]) & 0x80) != 0) {
            varint_offset++;
        }
        varint_offset++;
    }

    // Decode the count (stored as count-2, so add 2 back)
    const uint32_t count_minus_2 = decode_varint(index.data(), varint_offset);
    return count_minus_2 + 2;
}
