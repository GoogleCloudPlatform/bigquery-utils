//
// Created by mepan on 7/10/20.
//

#ifndef ZETASQL_HELPER_TOKEN_TOKEN_H
#define ZETASQL_HELPER_TOKEN_TOKEN_H

#include "zetasql/public/parse_tokens.h"
#include "zetasql_helper/token/parse_token.pb.h"
#include "zetasql/public/parse_location_range.pb.h"
#include <vector>
#include <string>


namespace bigquery::utils::zetasql_helper {

// Tokenize a query into a list of ZetaSQL tokens.
absl::Status Tokenize(const std::string &query, std::vector<zetasql::ParseToken>& parse_tokens);

// Serialize a ZetaSQL token into its proto buffer, which is used to transmit
// through RPC service.
ParseTokenProto serialize_token(const zetasql::ParseToken &token);
}


#endif //ZETASQL_HELPER_TOKEN_TOKEN_H
