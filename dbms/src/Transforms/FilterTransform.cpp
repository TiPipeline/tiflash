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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>
#include <Transforms/FilterTransform.h>

namespace DB
{
FilterTransform::FilterTransform(
    const Block & input_header,
    const ExpressionActionsPtr & expression_,
    const String & filter_column_name)
    : expression(expression_)
    , header(input_header)
{
    expression->execute(header);

    filter_column = header.getPositionByName(filter_column_name);
    auto & column_elem = header.safeGetByPosition(filter_column);

    /// Isn't the filter already constant?
    if (column_elem.column)
        constant_filter_description = ConstantFilterDescription(*column_elem.column);

    if (!constant_filter_description.always_false && !constant_filter_description.always_true)
    {
        /// Replace the filter column to a constant with value 1.
        FilterDescription filter_description_check(*column_elem.column);
        column_elem.column = column_elem.type->createColumnConst(header.rows(), UInt64(1));
    }
}

void FilterTransform::transformHeader(Block & block)
{
    block = header;
}

bool FilterTransform::transform(Block & block)
{
    if (unlikely(!block))
        return true;

    if (constant_filter_description.always_false)
    {
        return false;
    }

    if (constant_filter_description.always_true)
        return true;

    expression->execute(block);

    size_t columns = block.columns();
    size_t rows = block.rows();
    ColumnPtr column_of_filter = block.safeGetByPosition(filter_column).column;

    /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
        *  and now - are calculated. That is, not all cases are covered by the code above.
        * This happens if the function returns a constant for a non-constant argument.
        * For example, `ignore` function.
        */
    constant_filter_description = ConstantFilterDescription(*column_of_filter);

    if (constant_filter_description.always_false)
    {
        return false;
    }

    IColumn::Filter * filter;
    ColumnPtr filter_holder;

    if (constant_filter_description.always_true)
    {
        return true;
    }
    else
    {
        FilterDescription filter_and_holder(*column_of_filter);
        filter = const_cast<IColumn::Filter *>(filter_and_holder.data);
        filter_holder = filter_and_holder.data_holder;
    }

    /** Let's find out how many rows will be in result.
      * To do this, we filter out the first non-constant column
      *  or calculate number of set bytes in the filter.
      */
    size_t first_non_constant_column = 0;
    for (size_t i = 0; i < columns; ++i)
    {
        if (!block.safeGetByPosition(i).column->isColumnConst())
        {
            first_non_constant_column = i;

            if (first_non_constant_column != static_cast<size_t>(filter_column))
                break;
        }
    }

    size_t filtered_rows = 0;
    if (first_non_constant_column != static_cast<size_t>(filter_column))
    {
        ColumnWithTypeAndName & current_column = block.safeGetByPosition(first_non_constant_column);
        current_column.column = current_column.column->filter(*filter, -1);
        filtered_rows = current_column.column->size();
    }
    else
    {
        filtered_rows = countBytesInFilter(*filter);
    }

    /// If the current block is completely filtered out, let's move on to the next one.
    if (filtered_rows == 0)
        return false;

    /// If all the rows pass through the filter.
    if (filtered_rows == rows)
    {
        /// Replace the column with the filter by a constant.
        block.safeGetByPosition(filter_column).column
            = block.safeGetByPosition(filter_column).type->createColumnConst(filtered_rows, UInt64(1));
        /// No need to touch the rest of the columns.
        return true;
    }

    /// Filter the rest of the columns.
    for (size_t i = 0; i < columns; ++i)
    {
        ColumnWithTypeAndName & current_column = block.safeGetByPosition(i);

        if (i == static_cast<size_t>(filter_column))
        {
            /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
            /// NOTE User could pass column with something different than 0 and 1 for filter.
            /// Example:
            ///  SELECT materialize(100) AS x WHERE x
            /// will work incorrectly.
            current_column.column = current_column.type->createColumnConst(filtered_rows, UInt64(1));
            continue;
        }

        if (i == first_non_constant_column)
            continue;

        if (current_column.column->isColumnConst())
            current_column.column = current_column.column->cut(0, filtered_rows);
        else
            current_column.column = current_column.column->filter(*filter, filtered_rows);
    }

    return true;
}
} // namespace DB