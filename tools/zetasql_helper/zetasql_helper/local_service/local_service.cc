//
// Created by mepan on 8/22/20.
//

#include "local_service.h"
#include "zetasql_helper/token/token.h"

namespace bigquery::utils::zetasql_helper::local_service {

absl::Status ZetaSqlHelperLocalServiceImpl::Tokenize(
    const TokenizeRequest *request,
    TokenizeResponse *response) {

  std::vector<zetasql::ParseToken> tokens;
  ZETASQL_RETURN_IF_ERROR(::bigquery::utils::zetasql_helper::Tokenize(request->query(), tokens));

  for (auto &token : tokens) {
    auto token_proto = ::bigquery::utils::zetasql_helper::serialize_token(token);
    response->add_parse_token()->CopyFrom(token_proto);
  }
  return absl::OkStatus();
}

}//bigquery::utils::zetasql_helper::local_service

