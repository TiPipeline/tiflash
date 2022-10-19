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

#include <Transforms/AggregateStore.h>
#include <Transforms/Source.h>

namespace DB
{
class AggregateSource : public Source
{
public:
    explicit AggregateSource(
        const AggregateStorePtr & agg_store_)
        : agg_store(agg_store_)
    {}

    Block read() override
    {
        return agg_store->readForMerge();
    }

    Block getHeader() const override
    {
        return agg_store->getHeader();
    }

private:
    AggregateStorePtr agg_store;
};
} // namespace DB
