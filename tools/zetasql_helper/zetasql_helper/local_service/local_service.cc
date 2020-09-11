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

#include "local_service.h"
#include "zetasql_helper/token/token.h"
#include "zetasql_helper/scanner/extract_function.h"
#include "zetasql/parser/keywords.h"
#include "zetasql_helper/scanner/locate_table.h"
#include "zetasql_helper/fixer/fix_column_not_grouped.h"
#include "zetasql_helper/fixer/fix_duplicate_columns.h"

namespace bigquery::utils::zetasql_helper::local_service {

absl::Status ZetaSqlHelperLocalServiceImpl::Tokenize(
    const TokenizeRequest& request,
    TokenizeResponse* response) {

  std::vector<zetasql::ParseToken> tokens;
  ZETASQL_RETURN_IF_ERROR(::bigquery::utils::zetasql_helper::Tokenize(request.query(), tokens));

  for (auto &token : tokens) {
    auto token_proto = ::bigquery::utils::zetasql_helper::serialize_token(token);
    response->add_parse_tokens()->CopyFrom(token_proto);
  }
  return absl::OkStatus();
}

absl::Status ZetaSqlHelperLocalServiceImpl::ExtractFunctionRange(
    const ExtractFunctionRangeRequest& request,
    ExtractFunctionRangeResponse* response) {

  std::unique_ptr<::bigquery::utils::zetasql_helper::FunctionRange> output;
  ZETASQL_RETURN_IF_ERROR(
      ::bigquery::utils::zetasql_helper::ExtractFunctionRange(
          request.query(), request.line_number(), request.column_number(), &output
      ));

  auto status_or_function_range_proto = output->ToProto();
  ZETASQL_RETURN_IF_ERROR(status_or_function_range_proto.status());

  // Convert the proto to heap value and hand over the ownership to the response.
  auto function_range_proto = new ::bigquery::utils::zetasql_helper::FunctionRangeProto(
      status_or_function_range_proto.value()
  );
  response->set_allocated_function_range(function_range_proto);
  return absl::OkStatus();
}

absl::Status ZetaSqlHelperLocalServiceImpl::LocateTableRanges(const LocateTableRangesRequest& request,
                                                              LocateTableRangesResponse* response) {
  std::vector<zetasql::ParseLocationRange> ranges;
  ZETASQL_RETURN_IF_ERROR(
      ::bigquery::utils::zetasql_helper::LocateTableRanges(request.query(), request.table_regex(), ranges)
  );
  for (const auto& range : ranges) {
    ZETASQL_ASSIGN_OR_RETURN(auto value, range.ToProto());
    response->add_table_ranges()->CopyFrom(value);
  }

  return absl::Status();
}

absl::Status ZetaSqlHelperLocalServiceImpl::GetAllKeywords(const GetAllKeywordsRequest& request,
                                                           GetAllKeywordsResponse* response) {
  auto keywordInfos = zetasql::parser::GetAllKeywords();
  for (const auto& keywordInfo : keywordInfos) {
    response->add_keywords(keywordInfo.keyword());
  }

  return absl::Status();
}


absl::Status ZetaSqlHelperLocalServiceImpl::FixColumnNotGrouped(
    const FixColumnNotGroupedRequest& request,
    FixColumnNotGroupedResponse* response) {

  ZETASQL_ASSIGN_OR_RETURN(
      auto fixed_query,
      ::bigquery::utils::zetasql_helper::FixColumnNotGrouped(
          request.query(), request.missing_column(), request.line_number(), request.column_number()
      ));

  response->set_fixed_query(fixed_query);
  return absl::OkStatus();
}

absl::Status ZetaSqlHelperLocalServiceImpl::FixDuplicateColumns(
    const FixDuplicateColumnsRequest& request,
    FixDuplicateColumnsResponse* response) {

  ZETASQL_ASSIGN_OR_RETURN(
      auto fixed_query,
      ::bigquery::utils::zetasql_helper::FixDuplicateColumns(
          request.query(), request.duplicate_column()
      ));

  response->set_fixed_query(fixed_query);
  return absl::OkStatus();
}

}//bigquery::utils::zetasql_helper::local_service

