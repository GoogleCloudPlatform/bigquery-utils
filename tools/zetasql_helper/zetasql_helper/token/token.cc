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

#include "token.h"

namespace bigquery::utils::zetasql_helper {

absl::Status Tokenize(const std::string &query, std::vector<zetasql::ParseToken>& parse_tokens) {
  auto resume_location = zetasql::ParseResumeLocation::FromString(query);
  auto options = zetasql::ParseTokenOptions();
  ZETASQL_RETURN_IF_ERROR(GetParseTokens(options, &resume_location, &parse_tokens));
  return absl::OkStatus();
}

// Serialize the ZetaSQL token's kind into its proto. It is used by the `serialize_token`
// method.
ParseTokenProto_Kind serialize_token_kind(const zetasql::ParseToken::Kind kind) {
  using zetasql::ParseToken;
  using bigquery::utils::zetasql_helper::ParseTokenProto_Kind;
  switch (kind) {
    case ParseToken::Kind::KEYWORD:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_KEYWORD;
    case ParseToken::IDENTIFIER:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_IDENTIFIER;
    case ParseToken::IDENTIFIER_OR_KEYWORD:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_IDENTIFIER_OR_KEYWORD;
    case ParseToken::VALUE:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_VALUE;
    case ParseToken::COMMENT:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_COMMENT;
    case ParseToken::END_OF_INPUT:
      return ParseTokenProto_Kind::ParseTokenProto_Kind_END_OF_INPUT;
  }
}

ParseTokenProto serialize_token(const zetasql::ParseToken &token) {
  ParseTokenProto token_proto;
  token_proto.set_image(std::string(token.GetImage()));
  auto kind = serialize_token_kind(token.kind());
  token_proto.set_kind(kind);

  auto location_range_proto = token.GetLocationRange().ToProto();
  if (location_range_proto.ok()) {
    auto range_proto = new zetasql::ParseLocationRangeProto(location_range_proto.value());
    token_proto.set_allocated_parse_location_range(range_proto);
  }

  return token_proto;
}

} //bigquery::utils::zetasql_helper