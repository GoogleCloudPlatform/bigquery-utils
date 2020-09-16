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
#include "fix_duplicate_columns.h"
#include "fix_column_not_grouped.h"

using namespace bigquery::utils::zetasql_helper;

class FixerTest : public ::testing::Test {

};


TEST_F(FixerTest, FixDuplicateColumns) {
  absl::string_view query = "SELECT status, status FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
  absl::string_view duplicate_column = "status";

  auto status_or_value = FixDuplicateColumns(query, duplicate_column);
  ASSERT_TRUE(status_or_value.ok());

  auto fixed_query = status_or_value.value();
  EXPECT_EQ("SELECT\n"
            "  status AS status_1,\n"
            "  status AS status_2\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "LIMIT 1000\n", fixed_query);
}

TEST_F(FixerTest, FixColumnNotGrouped_createGroupByClause) {
  absl::string_view query = "SELECT status, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
  absl::string_view missing_column = "status";
  int line_number = 1;
  int col_number = 8;

  auto status_or_value = FixColumnNotGrouped(query, missing_column, line_number, col_number);
  ASSERT_TRUE(status_or_value.ok());

  auto fixed_query = status_or_value.value();
  EXPECT_EQ("SELECT\n"
            "  status,\n"
            "  max(unique_key)\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "GROUP BY status\n"
            "LIMIT 1000\n", fixed_query);
}

TEST_F(FixerTest, FixColumnNotGrouped_updateGroupByClause) {
  absl::string_view query = "SELECT status, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` group by city LIMIT 1000";
  absl::string_view missing_column = "status";
  int line_number = 1;
  int col_number = 8;

  auto status_or_value = FixColumnNotGrouped(query, missing_column, line_number, col_number);
  ASSERT_TRUE(status_or_value.ok());

  auto fixed_query = status_or_value.value();
  EXPECT_EQ("SELECT\n"
            "  status,\n"
            "  max(unique_key)\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "GROUP BY city, status\n"
            "LIMIT 1000\n", fixed_query);
}

TEST_F(FixerTest, FixColumnNotGrouped_columnWithKeywordName) {
  absl::string_view query = "SELECT `select`, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
  absl::string_view missing_column = "select";
  int line_number = 1;
  int col_number = 8;

  auto status_or_value = FixColumnNotGrouped(query, missing_column, line_number, col_number);
  ASSERT_TRUE(status_or_value.ok());

  auto fixed_query = status_or_value.value();
  EXPECT_EQ("SELECT\n"
            "  `select`,\n"
            "  max(unique_key)\n"
            "FROM\n"
            "  `bigquery-public-data.austin_311.311_request`\n"
            "GROUP BY `select`\n"
            "LIMIT 1000\n", fixed_query);
}


TEST_F(FixerTest, FixColumnNotGroupedTest4) {
  absl::string_view query = "SELECT `hash`,  mod(size, 100) as bucket FROM `bigquery-public-data.crypto_bitcoin.blocks` group by bucket, mod(number, 10) LIMIT 1000 ";
  absl::string_view missing_column = "`hash`";
  int line_number = 1;
  int col_number = 8;

  auto status_or_value = FixColumnNotGrouped(query, missing_column, line_number, col_number);
  ASSERT_TRUE(status_or_value.ok());

  auto fixed_query = status_or_value.value();
  EXPECT_EQ("SELECT\n"
            "  `hash`,\n"
            "  mod(size, 100) AS bucket\n"
            "FROM\n"
            "  `bigquery-public-data.crypto_bitcoin.blocks`\n"
            "GROUP BY bucket, mod(number, 10), `hash`\n"
            "LIMIT 1000\n", fixed_query);
}