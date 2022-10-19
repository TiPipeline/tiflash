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

#include <Common/Exception.h>
#include <Transforms/Transforms.h>

namespace DB
{
void Transforms::setSource(const SourcePtr & source_)
{
    assert(!source);
    source = source_;
    assert(source);
}
void Transforms::setSink(const SinkPtr & sink_)
{
    assert(!sink);
    sink = sink_;
    assert(sink);
}
void Transforms::append(const TransformPtr & transform)
{
    transforms.emplace_back(transform);
}

void Transforms::addTableLock(const TableLockHolder & lock)
{
    table_locks.push_back(lock);
}

bool Transforms::execute()
{
    assert(source);
    assert(sink);

    auto block = source->read();
    for (const auto & transform : transforms)
    {
        if (!transform->transform(block))
            return true;
    }
    return sink->write(block);
}

void Transforms::prepare()
{
    source->prepare();
}

void Transforms::finish()
{
    sink->finish();
    source->finish();
}

void Transforms::cancel(bool kill)
{
    assert(source);
    source->cancel(kill);
}

bool Transforms::isIOReady()
{
    return sink->isIOReady() && source->isIOReady();
}

Block Transforms::getHeader()
{
    assert(source);
    Block block = source->getHeader();
    for (const auto & transform : transforms)
        transform->transformHeader(block);
    return block;
}
} // namespace DB
