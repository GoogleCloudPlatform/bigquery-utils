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