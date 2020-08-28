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

