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

#include "gtest/gtest.h"
#include "zetasql_helper/scanner/locate_table.h"
#include "zetasql_helper/scanner/extract_function.h"

using namespace bigquery::utils::zetasql_helper;

class LocationTest : public ::testing::Test {

};

std::string range_to_string(absl::string_view query, zetasql::ParseLocationRange& range) {
  auto start = range.start().GetByteOffset();
  auto length = range.end().GetByteOffset() - range.start().GetByteOffset();
  return std::string(query.substr(start, length));
}

TEST_F(LocationTest, LocateTableTest1) {
  std::string query =
      "SELECT `特殊字符 (unicode characters)`, status FROM bigquery-public-data.`austin_311.311_request`"
      "cross join `austin_311`.311_request\n"
      "where status = '`bigquery-public-data.austin_311.311_request`'";

  std::string table_regex = "(bigquery-public-data.)?austin_311.311_request";

  std::vector<zetasql::ParseLocationRange> ranges;
  auto status = LocateTableRanges(query, table_regex, ranges);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(2, ranges.size());
  EXPECT_EQ("bigquery-public-data.`austin_311.311_request`", range_to_string(query, ranges[0]));
  EXPECT_EQ("`austin_311`.311_request", range_to_string(query, ranges[1]));
}


TEST_F(LocationTest, LocateTableTest2) {
  std::string query = "Select max(foo) from bigquery-public-data.mock.survey_2017 group by bar limit 10";
  std::string table_regex = "bigquery-public-data\\.mock\\.survey_2017";

  std::vector<zetasql::ParseLocationRange> ranges;
  auto status = LocateTableRanges(query, table_regex, ranges);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(1, ranges.size());
//  EXPECT_EQ(2, ranges[0].start().GetByteOffset())
}

TEST_F(LocationTest, ExtractFunction) {
  std::string query = "select \"特殊字符 (unicode characters)\", * from (select timestamp(10232) as x)";
  int line = 1;
  int col = 52;

  std::unique_ptr<FunctionRange> output;
  auto status = ExtractFunctionRange(query, line, col, &output);

  ASSERT_TRUE(status.ok());
  EXPECT_EQ("timestamp", range_to_string(query, output->name));
  EXPECT_EQ("timestamp(10232)", range_to_string(query, output->function));
  EXPECT_EQ(1, output->arguments.size());
  EXPECT_EQ("10232", range_to_string(query, output->arguments[0]));

}

