// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Block.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/Join.h>

#include <memory>

namespace DB
{
struct PreBuildUnit
{
    std::mutex mutex;
    Blocks pre_build_blocks;
    size_t rows = 0;
    size_t inserter_count = 0;

    void insert(Block && block)
    {
        std::lock_guard lock(mutex);
        rows += block.rows();
        pre_build_blocks.emplace_back(std::move(block));
    }

    void countInserter()
    {
        std::lock_guard lock(mutex);
        ++inserter_count;
    }

    Block popBack()
    {
        std::lock_guard lock(mutex);
        if (pre_build_blocks.empty())
            return {};
        Block block = std::move(pre_build_blocks.back());
        pre_build_blocks.pop_back();
        return block;
    }

    void doReserve(const JoinPtr & join, size_t index)
    {
        bool can_reserve = false;
        size_t reserve_rows = 0;
        {
            std::lock_guard lock(mutex);
            can_reserve = (0 == --inserter_count);
            reserve_rows = rows;
        }
        if (can_reserve)
            join->reserve(reserve_rows, index);
    }
};

class JoinPreBuilder
{
public:
    explicit JoinPreBuilder(const Block & sample_block): header(materializeBlock(sample_block)) {}

    void initForWrite(size_t concurrency)
    {
        std::unique_lock init_lock(init_mutex);
        if (inited)
            return;
        inited = true;

        max_threads = concurrency;
        assert(max_threads > 0);
        pre_build_units.reserve(max_threads);
        for (size_t i = 0; i < max_threads; ++i)
            pre_build_units.emplace_back(std::make_unique<PreBuildUnit>());
    }

    void countInserter(size_t index)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        assert(index < max_threads);
        pre_build_units[index]->countInserter();
    }

    void insert(size_t index, Block && block)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        assert(index < max_threads);
        pre_build_units[index]->insert(std::move(block));
    }

    Block popBack(size_t index)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        assert(index < max_threads);
        return pre_build_units[index]->popBack();
    }

    void doReserve(const JoinPtr & join, size_t index)
    {
        std::shared_lock init_lock(init_mutex);
        assert(inited);
        pre_build_units[index]->doReserve(join, index);
    }

    const Block & getHeader() const
    {
        return header;
    }

public:
    size_t max_threads;

private:
    Block header;

    mutable std::shared_mutex init_mutex;
    bool inited = false;
    std::vector<std::unique_ptr<PreBuildUnit>> pre_build_units;
};
using JoinPreBuilderPtr = std::shared_ptr<JoinPreBuilder>;
} // namespace DB
