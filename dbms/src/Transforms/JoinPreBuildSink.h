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

#include <Interpreters/Join.h>
#include <Transforms/JoinPreBuilder.h>
#include <Transforms/Sink.h>

namespace DB
{
class JoinPreBuildSink : public Sink
{
public:
    JoinPreBuildSink(
        const JoinPtr & join_,
        const JoinPreBuilderPtr & join_pre_builder_,
        size_t index_)
        : join(join_)
        , join_pre_builder(join_pre_builder_)
        , index(index_)
    {
        join_pre_builder->countInserter(index);
    }

    bool write(Block & block) override
    {
        if (unlikely(!block))
            return false;

        join_pre_builder->insert(index, std::move(block));
        return true;
    }

    void finish() override
    {
        join_pre_builder->doReserve(join, index);
    }

private:
    JoinPtr join;
    JoinPreBuilderPtr join_pre_builder;
    size_t index;
};
} // namespace DB
