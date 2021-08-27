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

#ifndef ZETASQL_HELPER_FIX_DUPLICATE_COLUMNS_H
#define ZETASQL_HELPER_FIX_DUPLICATE_COLUMNS_H

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/parser/parse_tree.h"

namespace bigquery::utils::zetasql_helper {

// Fix the "Duplicate Column" error. It will add numeric suffix (e.g. _1, _2, ..) to the duplicate name
// as the new aliases for each duplicate columns.
// Input:
// query: incorrect SQL with the error
// duplicate_column_name: name of the duplicate columns
//
// The function will return the fixed query.
zetasql_base::StatusOr<std::string>
FixDuplicateColumns(absl::string_view query, absl::string_view duplicate_column_name);


// Find the ASTSelectList Node having duplicate columns, whose name is the input name.
const zetasql::ASTSelectList*
FindSelectListWithDuplicateColumns(const zetasql::ASTNode& node, absl::string_view column_name);

// Get the column name of an ASTSelectColumn node.
std::string GetColumnName(const zetasql::ASTSelectColumn* column_node);

// Change the duplicate names to their new names by adding numeric suffix after them.
void
ReplaceDuplicateColumnsOfSelectList(const zetasql::ASTSelectList& select_list, absl::string_view column_name,
                                    zetasql_base::UnsafeArena* arena, zetasql::IdStringPool* id_string_pool);

// Update or create an alias node of an ASTSelectColumn
void UpdateAlias(zetasql::ASTSelectColumn* column_node, absl::string_view new_alias,
                 zetasql_base::UnsafeArena* arena, zetasql::IdStringPool* id_string_pool);

// Create an alias node of an ASTSelectColumn
zetasql::ASTAlias* CreateAliasNode(absl::string_view new_alias, zetasql_base::UnsafeArena* arena,
                                   zetasql::IdStringPool* id_string_pool);

} // bigquery::utils::zetasql_helper

#endif //ZETASQL_HELPER_FIX_DUPLICATE_COLUMNS_H
