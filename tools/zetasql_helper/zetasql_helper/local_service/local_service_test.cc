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

#include "googletest/include/gtest/gtest.h"
#include "zetasql_helper/local_service/local_service_grpc.h"
#include "zetasql/public/parse_location.h"

namespace bigquery::utils::zetasql_helper::local_service {

class LocalServiceTest : public ::testing::Test {

 public:
  ZetaSqlHelperLocalServiceGrpcImpl& GetService() {
    return service_;
  }

 private:
  ZetaSqlHelperLocalServiceGrpcImpl service_;
};

TEST_F(LocalServiceTest, Tokenize) {
  std::string query = "select 1 foo";
  TokenizeRequest request;
  TokenizeResponse response;
  request.set_query(query);
  GetService().Tokenize(nullptr, &request, &response);

  auto tokens = response.parse_tokens();

  EXPECT_EQ(4, tokens.size());
  EXPECT_EQ("select", tokens[0].image());
  EXPECT_EQ(::bigquery::utils::zetasql_helper::ParseTokenProto_Kind::ParseTokenProto_Kind_KEYWORD, tokens[0].kind());
  EXPECT_EQ("1", tokens[1].image());
  EXPECT_EQ("foo", tokens[2].image());
}

std::string range_to_string(absl::string_view query, const zetasql::ParseLocationRangeProto& range_proto) {
  auto range = zetasql::ParseLocationRange::Create(range_proto).value();
  auto start = range.start().GetByteOffset();
  auto length = range.end().GetByteOffset() - range.start().GetByteOffset();
  return std::string(query.substr(start, length));
}

TEST_F(LocalServiceTest, ExtractFunctionRange) {
  std::string query = "select foo.bar((select a from b), \", a, b, c\", foo.bar(1,2,3))";
  ExtractFunctionRangeRequest request;
  ExtractFunctionRangeResponse response;
  request.set_query(query);
  request.set_line_number(1);
  request.set_column_number(8);
  GetService().ExtractFunctionRange(nullptr, &request, &response);

  auto function_range = response.function_range();

  EXPECT_EQ("foo.bar((select a from b), \", a, b, c\", foo.bar(1,2,3))",
            range_to_string(query, function_range.function()));
  EXPECT_EQ("foo.bar", range_to_string(query, function_range.name()));
  EXPECT_EQ("(select a from b)", range_to_string(query, function_range.arguments(0)));
  EXPECT_EQ("\", a, b, c\"", range_to_string(query, function_range.arguments(1)));
  EXPECT_EQ("foo.bar(1,2,3)", range_to_string(query, function_range.arguments(2)));
}

TEST_F(LocalServiceTest, LocateTableRanges) {
  std::string query = "SELECT `特殊字符 (unicode characters)`, status FROM bigquery-public-data.`austin_311.311_request`"
                      "cross join `austin_311`.311_request\n"
                      "where status = '`bigquery-public-data.austin_311.311_request`'";
  LocateTableRangesRequest request;
  LocateTableRangesResponse response;
  request.set_query(query);
  request.set_table_regex("(bigquery-public-data.)?austin_311.311_request");
  GetService().LocateTableRanges(nullptr, &request, &response);

  EXPECT_EQ(2, response.table_ranges().size());
  EXPECT_EQ("bigquery-public-data.`austin_311.311_request`", range_to_string(query, response.table_ranges()[0]));
  EXPECT_EQ("`austin_311`.311_request", range_to_string(query, response.table_ranges()[1]));
}


TEST_F(LocalServiceTest, GetAllKeywords) {
  GetAllKeywordsRequest request;
  GetAllKeywordsResponse response;
  GetService().GetAllKeywords(nullptr, &request, &response);

  EXPECT_EQ(231, response.keywords().size());
}

TEST_F(LocalServiceTest, FixDuplicateColumns) {
  std::string query = "SELECT status, status FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
  std::string duplicate_column = "status";

  FixDuplicateColumnsRequest request;
  request.set_query(query);
  request.set_duplicate_column(duplicate_column);

  FixDuplicateColumnsResponse response;
  GetService().FixDuplicateColumns(nullptr, &request, &response);


  EXPECT_EQ("SELECT\n"
            "  status AS status_1,\n"
            "  status AS status_2\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "LIMIT 1000\n", response.fixed_query());
}

TEST_F(LocalServiceTest, FixColumnNotGrouped) {
  std::string query = "SELECT status, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
  std::string missing_column = "status";
  int line_number = 1;
  int col_number = 8;

  FixColumnNotGroupedRequest request;
  request.set_query(query);
  request.set_missing_column(missing_column);
  request.set_line_number(line_number);
  request.set_column_number(col_number);

  FixColumnNotGroupedResponse response;
  GetService().FixColumnNotGrouped(nullptr, &request, &response);


  EXPECT_EQ("SELECT\n"
            "  status,\n"
            "  max(unique_key)\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "GROUP BY status\n"
            "LIMIT 1000\n", response.fixed_query());
}
}

