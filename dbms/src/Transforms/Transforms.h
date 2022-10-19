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
#include <Storages/TableLockHolder.h>
#include <Transforms/Sink.h>
#include <Transforms/Source.h>

#include <atomic>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

class Transform
{
public:
    virtual ~Transform() = default;

    virtual bool transform(Block & block) = 0;
    virtual void transformHeader(Block & header) { transform(header); }
};
using TransformPtr = std::shared_ptr<Transform>;

class Transforms
{
public:
    void setSource(const SourcePtr & source_);
    void setSink(const SinkPtr & sink_);
    void append(const TransformPtr & transform);

    bool execute();

    void prepare();
    void finish();

    void cancel(bool kill);

    Block getHeader();

    void addTableLock(const TableLockHolder & lock);

    bool isIOReady();

private:
    TableLockHolders table_locks;

    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;
};
using TransformsPtr = std::shared_ptr<Transforms>;
} // namespace DB
