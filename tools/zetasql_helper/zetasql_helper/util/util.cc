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

#include "util.h"
#include "zetasql/public/parse_helpers.h"
#include "absl/strings/strip.h"
#include "absl/strings/str_split.h"

namespace bigquery::utils::zetasql_helper {

zetasql::AnalyzerOptions BigQueryOptions() {
  zetasql::AnalyzerOptions options;
  zetasql::LanguageOptions language_options;
  // Latest language version.
  language_options.SetLanguageVersion(zetasql::LanguageVersion::VERSION_1_3);
  options.set_language_options(language_options);
  return options;
}

int get_offset(absl::string_view query, int line_number, int column_number) {
  zetasql::ParseLocationTranslator translator(query);
  auto result = translator.GetByteOffsetFromLineAndColumn(line_number, column_number);

  if (result.ok()) {
    return result.value();
  } else {
    return -1;
  }
}

absl::string_view RemoveBacktick(absl::string_view column) {
  column = absl::StripPrefix(column, "`");
  column = absl::StripSuffix(column, "`");
  return column;
}

std::vector<std::string> ReadNames(const zetasql::ASTPathExpression &path) {
  std::vector<std::string> names;
  for (auto child : path.names()) {
    auto identifier = child->GetAsString();
    auto sub_names = absl::StrSplit(RemoveBacktick(identifier), '.');
    for (auto name : sub_names) {
      names.push_back(std::string(name));
    }
  }
  return names;
}

} // bigquery::utils::zetasql_helper