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

#include "fix_duplicate_columns.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql_helper/util/util.h"


namespace bigquery::utils::zetasql_helper {

zetasql_base::StatusOr<std::string>
FixDuplicateColumns(absl::string_view query, absl::string_view duplicate_column_name) {
  std::unique_ptr<zetasql::ParserOutput> parser_output;
  auto options = BigQueryOptions();
  ZETASQL_RETURN_IF_ERROR(ParseStatement(query, options.GetParserOptions(), &parser_output));

  duplicate_column_name = RemoveBacktick(duplicate_column_name);

  auto select_list = FindSelectListWithDuplicateColumns(*parser_output->statement(), duplicate_column_name);
  if (select_list == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "Duplicate columns does not exist.");
  }

  ReplaceDuplicateColumnsOfSelectList(*select_list, duplicate_column_name, parser_output->arena().get(),
                                      parser_output->id_string_pool().get());

  return Unparse(parser_output->statement());
}

const zetasql::ASTSelectList*
FindSelectListWithDuplicateColumns(const zetasql::ASTNode& node, absl::string_view column_name) {
  // Set up the predicator to find the target node.
  auto predicator = [column_name](const zetasql::ASTNode* node) {
    if (node->node_kind() != zetasql::ASTNodeKind::AST_SELECT_LIST) {
      return false;
    }
    auto select_list = dynamic_cast<const zetasql::ASTSelectList*>(node);

    // Count how many columns have the input name
    int count = 0;

    for (auto column : select_list->columns()) {
      if (column_name == GetColumnName(column)) {
        count++;
      }
    }

    // count > 1 means duplicate columns.
    if (count > 1) {
      return true;
    }

  };

  auto candidate = FindNode(&node, predicator);
  return dynamic_cast<const zetasql::ASTSelectList*>(candidate);

}

std::string GetColumnName(const zetasql::ASTSelectColumn* column_node) {
  if (column_node == nullptr) {
    return "";
  }

  // If alias is set, return the alias
  auto alias = column_node->alias();
  if (alias != nullptr) {
    return alias->identifier()->GetAsString();
  }

  for (int i = 0; i < column_node->num_children(); i++) {
    auto child = column_node->child(i);
    if (child->node_kind() == zetasql::ASTNodeKind::AST_ALIAS) {
      continue;
    }

    auto path_expression = dynamic_cast<const zetasql::ASTPathExpression*>(child);

    // If the child is of ASTSelectColumn is not ASTPathExpression, then the name should be empty
    if (path_expression == nullptr) {
      return "";
    }

    // Return the last name of ASTPathExpression as the column name.
    return path_expression->last_name()->GetAsString();
  }
}

void ReplaceDuplicateColumnsOfSelectList(
    const zetasql::ASTSelectList& select_list,
    absl::string_view column_name,
    zetasql_base::UnsafeArena* arena,
    zetasql::IdStringPool* id_string_pool
) {
  std::vector<zetasql::ASTSelectColumn*> duplicate_columns;

  // Find all the duplicate columns
  for (auto column : select_list.columns()) {
    if (column_name == GetColumnName(column)) {
      duplicate_columns.push_back(const_cast<zetasql::ASTSelectColumn*>(column));
    }
  }

  for (int i = 0; i < duplicate_columns.size(); i++) {
    auto new_alias = absl::StrCat(column_name, "_", std::to_string(i + 1));
    // rename the column with 1-based index
    UpdateAlias(duplicate_columns[i], new_alias, arena, id_string_pool);
  }
}

void UpdateAlias(
    zetasql::ASTSelectColumn* column_node,
    absl::string_view new_alias,
    zetasql_base::UnsafeArena* arena,
    zetasql::IdStringPool* id_string_pool
) {
  if (column_node->alias() == nullptr) {
    auto alias_node = CreateAliasNode(new_alias, arena, id_string_pool);
    column_node->AddChild(alias_node);
    ((zetasql::ASTNode*) column_node)->InitFields();
    return;
  }

  auto identifier = const_cast<zetasql::ASTIdentifier*>(column_node->alias()->identifier());
  identifier->SetIdentifier(id_string_pool->Make(new_alias));
  ((zetasql::ASTNode*) identifier)->InitFields();
}

zetasql::ASTAlias* CreateAliasNode(
    absl::string_view new_alias,
    zetasql_base::UnsafeArena* arena,
    zetasql::IdStringPool* id_string_pool
) {
  auto identifier = new(zetasql_base::AllocateInArena, arena) zetasql::ASTIdentifier;
  identifier->SetIdentifier(id_string_pool->Make(new_alias));
  ((zetasql::ASTNode*) identifier)->InitFields();

  auto alias_node = new(zetasql_base::AllocateInArena, arena) zetasql::ASTAlias;
  alias_node->AddChild(identifier);
  ((zetasql::ASTNode*) alias_node)->InitFields();
  return alias_node;
}

} // bigquery::utils::zetasql_helper

