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
