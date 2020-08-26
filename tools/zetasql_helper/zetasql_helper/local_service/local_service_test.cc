#include "googletest/include/gtest/gtest.h"
#include "zetasql_helper/local_service/local_service_grpc.h"

namespace bigquery::utils::zetasql_helper::local_service {

class LocalServiceTest : public ::testing::Test {

 public:
  ZetaSqlHelperLocalServiceGrpcImpl& GetService() {
    return service_;
  }

 private:
  ZetaSqlHelperLocalServiceGrpcImpl service_;
};

TEST_F(LocalServiceTest, TokenizeTest) {
  std::string query = "select 1 foo";
  TokenizeRequest request;
  TokenizeResponse response;
  request.set_query(query);
  GetService().Tokenize(nullptr, &request, &response);

  auto tokens = response.parse_token();

  EXPECT_EQ(4, tokens.size());
  EXPECT_EQ("select", tokens[0].image());
  EXPECT_EQ(::bigquery::utils::zetasql_helper::ParseTokenProto_Kind::ParseTokenProto_Kind_KEYWORD, tokens[0].kind());
  EXPECT_EQ("1", tokens[1].image());
  EXPECT_EQ("foo", tokens[2].image());
}

}

