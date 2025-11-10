#!/bin/bash
for q in 60 62 64 65 66 68 70; do
    echo "Q=$q"
    # 临时修改User.hpp中的Q值
    sed -i.bak "s/estimated_queries_per_block = [0-9.]*;/estimated_queries_per_block = $q.0;/" User.hpp
    make build > /dev/null 2>&1
    echo -n "1:1="
    ./main.out 1 1 ../data/sample 2>/dev/null | grep "total score" | awk '{print $3}'
    echo -n "1:10="
    ./main.out 1 10 ../data/sample 2>/dev/null | grep "total score" | awk '{print $3}'
    echo -n "10:1="
    ./main.out 10 1 ../data/sample 2>/dev/null | grep "total score" | awk '{print $3}'
    # 恢复原始文件
    mv User.hpp.bak User.hpp
    echo "---"
done
