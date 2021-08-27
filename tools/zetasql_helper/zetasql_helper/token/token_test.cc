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
#include "zetasql_helper/token/token.h"

using namespace bigquery::utils::zetasql_helper;

class TokenTest : public ::testing::Test {

};

TEST_F(TokenTest, TokenizeTest) {
  std::string query = "select 1 foo";
  std::vector<zetasql::ParseToken> tokens;
  auto status = ::bigquery::utils::zetasql_helper::Tokenize(query, tokens);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(4, tokens.size());
  EXPECT_EQ("select", tokens[0].GetImage());
  EXPECT_EQ(zetasql::ParseToken::Kind::KEYWORD, tokens[0].kind());
  EXPECT_EQ(1, tokens[1].GetValue().int64_value());
  EXPECT_EQ("foo", tokens[2].GetImage());
}