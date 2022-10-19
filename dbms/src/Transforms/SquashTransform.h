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

#include <Interpreters/ExpressionActions.h>
#include <Transforms/Transforms.h>

namespace DB
{
class SquashTransform : public Transform
{
public:
    explicit SquashTransform(
        size_t min_block_size_rows,
        size_t min_block_size_bytes,
        const String & req_id)
        : squashing_transform(min_block_size_rows, min_block_size_bytes, req_id)
    {}

    bool transform(Block & block) override
    {
        SquashingTransform::Result result = squashing_transform.add(std::move(block));
        if (result.ready)
        {
            block = std::move(result.block);
            return true;
        }
        else
        {
            return false;
        }
    }

    void transformHeader(Block &) override {}

private:
    SquashingTransform squashing_transform;
};
} // namespace DB
