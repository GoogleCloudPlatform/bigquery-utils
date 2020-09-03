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

#include "locate_table.h"
#include "zetasql/parser/parser.h"
#include "zetasql_helper/util/util.h"
#include "absl/strings/str_join.h"
#include <regex>

namespace bigquery::utils::zetasql_helper {

absl::Status LocateTableRanges(absl::string_view query,
                               absl::string_view table_regex,
                               std::vector<zetasql::ParseLocationRange>& output) {

  std::unique_ptr<zetasql::ParserOutput> parser_output;
  auto options = BigQueryOptions();
  ZETASQL_RETURN_IF_ERROR(ParseStatement(query, options.GetParserOptions(), &parser_output));

  // Predicate to find a table whose name meets the table_rex
  auto find_table = [table_regex](const zetasql::ASTNode* node) {
    auto table_path = dynamic_cast<const zetasql::ASTTablePathExpression*>(node);
    if (table_path == nullptr) {
      return false;
    }

    // Read the full name of a table node.
    auto names = ReadNames(*table_path->path_expr());
    auto full_name = absl::StrJoin(names, ".");

    return std::regex_match(full_name, std::regex(std::string(table_regex)));
  };

  auto table_nodes = FindAllNodes(parser_output->statement(), find_table);
  for (const auto table_node : table_nodes) {
    output.push_back(table_node->GetParseLocationRange());
  }

  return absl::OkStatus();
}

}


