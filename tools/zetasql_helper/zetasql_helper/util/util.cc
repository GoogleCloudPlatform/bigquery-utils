//
// Created by mepan on 8/11/20.
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

absl::string_view remove_backtick(absl::string_view column) {
  column = absl::StripPrefix(column, "`");
  column = absl::StripSuffix(column, "`");
  return column;
}

std::vector<std::string> read_names(const zetasql::ASTPathExpression &path) {
  std::vector<std::string> names;
  for (auto child : path.names()) {
    auto identifier = child->GetAsString();
    auto sub_names = absl::StrSplit(remove_backtick(identifier), '.');
    for (auto name : sub_names) {
      names.push_back(std::string(name));
    }
  }
  return names;
}

} // bigquery::utils::zetasql_helper