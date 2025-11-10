#include <optional>
#include <vector>
#include <span>
#include <unordered_map>
#include <algorithm>

#include "Parameters.hpp"

inline size_t encode_varint(uint32_t value, std::byte* output) {
    size_t bytes = 0;
    while (value >= 128) {
        output[bytes++] = static_cast<std::byte>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    output[bytes++] = static_cast<std::byte>(value & 0x7F);
    return bytes;
}

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

std::vector<std::byte> build_idx(std::span<const uint32_t> data, Parameters config){
    std::unordered_map<uint32_t, uint32_t> freq_map;
    for (const auto val : data) {
        freq_map[val]++;
    }

    const size_t num_distinct = freq_map.size();
    const double cost_ratio = static_cast<double>(config.f_a) / static_cast<double>(config.f_s);

    constexpr double estimated_queries_per_block = 550.0;

    std::vector<std::pair<uint32_t, uint32_t>> items;
    items.reserve(num_distinct);
    for (const auto& [value, count] : freq_map) {
        items.emplace_back(value, count);
    }
    std::ranges::sort(items,
                      [](const auto& a, const auto& b) { return a.first < b.first; });
    
    std::vector<std::byte> index;
    index.reserve(8 + num_distinct * 3);

    const auto num_distinct_u32 = static_cast<uint32_t>(num_distinct);
    index.resize(8);
    std::memcpy(index.data(), &num_distinct_u32, sizeof(uint32_t));
    size_t offset = 8;
    
    // Values: delta-varint (first value also varint)
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
    
    // Counts section
    const auto counts_offset = static_cast<uint32_t>(offset);
    std::memcpy(index.data() + 4, &counts_offset, sizeof(uint32_t));
    
    size_t bitmap_bytes = (num_distinct + 7) / 8;
    index.resize(offset + bitmap_bytes);
    std::memset(index.data() + offset, 0, bitmap_bytes);
    size_t bitmap_offset = offset;
    offset += bitmap_bytes;
    
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

    const auto actual_size_bytes = static_cast<double>(index.size());

    if (const double break_even_bytes = estimated_queries_per_block * cost_ratio * 1024.0; actual_size_bytes > break_even_bytes) {
        return {};
    }
    
    return index;
}

std::optional<size_t> query_idx(uint32_t predicate, const std::vector<std::byte>& index){
    if (index.empty()) {
        return std::nullopt;
    }
    
    uint32_t num_indexed;
    uint32_t counts_offset;
    std::memcpy(&num_indexed, index.data(), sizeof(uint32_t));
    std::memcpy(&counts_offset, index.data() + 4, sizeof(uint32_t));
    
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
        
        if (current_value > predicate) {
            return 0;
        }
    }
    
    if (found_idx < 0) {
        return 0;
    }

    const size_t bitmap_bytes = (num_indexed + 7) / 8;
    const size_t bitmap_offset = counts_offset;
    size_t varint_offset = counts_offset + bitmap_bytes;
    
    size_t byte_idx = found_idx / 8;
    size_t bit_idx = found_idx % 8;

    bool is_one = (static_cast<uint32_t>(index[bitmap_offset + byte_idx]) & (1 << bit_idx)) != 0;
    if (is_one) {
        return 1;
    }
    
    // Count non-ones before
    size_t non_ones_before = 0;
    for (int i = 0; i < found_idx; i++) {
        byte_idx = i / 8;
        bit_idx = i % 8;
        bool is_one_i = (static_cast<uint32_t>(index[bitmap_offset + byte_idx]) & (1 << bit_idx)) != 0;
        if (!is_one_i) {
            non_ones_before++;
        }
    }
    
    for (size_t i = 0; i < non_ones_before; i++) {
        while ((static_cast<uint32_t>(index[varint_offset]) & 0x80) != 0) {
            varint_offset++;
        }
        varint_offset++;
    }

    const uint32_t count_minus_2 = decode_varint(index.data(), varint_offset);
    return count_minus_2 + 2;
}
