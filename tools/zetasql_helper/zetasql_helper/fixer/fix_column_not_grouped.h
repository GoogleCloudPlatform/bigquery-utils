//
// Copyright 2020 BigQuery Utils
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef ZETASQL_HELPER_FIX_COLUMN_NOT_GROUPED_H
#define ZETASQL_HELPER_FIX_COLUMN_NOT_GROUPED_H

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/parser/parse_tree.h"


namespace bigquery::utils::zetasql_helper {

// Fix the "Column Not Grouped" error. It will add the missing (ungrouped) column to the corresponding
// group-by clause. If the group-by clause does not exist, it will create a new one.
// Input:
// query: incorrect SQL with the error
// missing_column: name of the missing (ungrouped) column
// line/column number: the starting position of the ungrouped column (1-based index)
//
// The function will return the fixed query.
zetasql_base::StatusOr<std::string>
FixColumnNotGrouped(absl::string_view query, absl::string_view missing_column, int line_number, int column_number);

// Find the AST_Select Node having a column node. The column node should start at the given offset
// and has the given column name.
const zetasql::ASTSelect*
FindSelectNodeHavingColumn(const zetasql::ASTStatement* statement, int column_start_offset,
                           absl::string_view column);

// Check if the input AST_Node pointer points to an AST_Path_Expression node whose column name is the
// input name.
// The input might be null_ptr.
bool IsPathExpression(const zetasql::ASTNode* node, absl::string_view name);


// Find an AST_Path_Expression node inside the given AST node. The AST_Path_Expression should have
// the staring offset and name identical to the input values.
// Return the AST_Path_Expression pointer if it is found. Otherwise, a null pointer will be returned.
const zetasql::ASTNode*
FindPathExpressionNode(const zetasql::ASTNode& node, int column_start_offset, absl::string_view name);


// Add a column to the group-by clause of the input AST_Select node.
// zetasql_base::UnsafeArena and zetasql::IdStringPool are two objects required to create a new node,
// they can be acquired from the zetasql::AnalyzerOptions.
void AddColumnToGroupByClause(zetasql::ASTSelect* select_node, absl::string_view column,
                              zetasql_base::UnsafeArena* arena, zetasql::IdStringPool* id_string_pool);


// Get or Create a AST_Group_By node at the given AST_Select node.
// zetasql_base::UnsafeArena* is used to allocate space for the created node, which can be acquired from the
// zetasql::AnalyzerOptions
zetasql::ASTGroupBy* GetOrCreateGroupByNode(zetasql::ASTSelect* select_node, zetasql_base::UnsafeArena* arena);

// Create a AST_Grouping_Column node..
// zetasql_base::UnsafeArena and zetasql::IdStringPool are two objects required to create a new node,
// they can be acquired from the zetasql::AnalyzerOptions.
zetasql::ASTGroupingItem* NewGroupingColumn(absl::string_view column, zetasql_base::UnsafeArena* arena,
                                            zetasql::IdStringPool* id_string_pool);


} // bigquery::utils::zetasql_helper

#endif //ZETASQL_HELPER_FIX_COLUMN_NOT_GROUPED_H
