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

#include "extract_function.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/parser/parser.h"
#include "zetasql_helper/util/util.h"

namespace bigquery::utils::zetasql_helper {

FunctionRange::FunctionRange(const zetasql::ASTFunctionCall& function_call) {
  function = function_call.GetParseLocationRange();
  name = function_call.function()->GetParseLocationRange();
  std::vector<zetasql::ParseLocationRange> ranges;
  for (auto argument : function_call.arguments()) {
    ranges.push_back(argument->GetParseLocationRange());
  }
  arguments = ranges;
}

zetasql_base::StatusOr<FunctionRangeProto> FunctionRange::ToProto() const {
  FunctionRangeProto proto;
  // location range transfer
  ZETASQL_ASSIGN_OR_RETURN(auto function_proto, function.ToProto());

  auto range_proto = new zetasql::ParseLocationRangeProto(function_proto);
  proto.set_allocated_function(range_proto);

  // name range transfer
  ZETASQL_ASSIGN_OR_RETURN(auto name_proto, name.ToProto());

  range_proto = new zetasql::ParseLocationRangeProto(name_proto);
  proto.set_allocated_name(range_proto);

  for (const auto& argument_range : arguments) {
    ZETASQL_ASSIGN_OR_RETURN(auto argument_proto, argument_range.ToProto());
    proto.add_arguments()->CopyFrom(argument_proto);
  }

  return proto;
}


absl::Status ExtractFunctionRange(absl::string_view query,
                                  int row,
                                  int column,
                                  std::unique_ptr<FunctionRange>* output) {

  std::unique_ptr<zetasql::ParserOutput> parser_output;
  auto options = BigQueryOptions();
  ZETASQL_RETURN_IF_ERROR(ParseStatement(query, options.GetParserOptions(), &parser_output));

  auto offset = get_offset(query, row, column);
  auto predicator = [offset](const zetasql::ASTNode* node) {
    return node->GetParseLocationRange().start().GetByteOffset() == offset &&
        node->node_kind() == zetasql::ASTNodeKind::AST_FUNCTION_CALL;
  };

  auto candidate = FindNode(parser_output->statement(), predicator);
  if (candidate == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "Line and/or column numbers are incorrect");
  }

  auto function_call = dynamic_cast<const zetasql::ASTFunctionCall*>(candidate);
  if (function_call == nullptr) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "The provided position is not function node.");
  }

  *output = absl::make_unique<FunctionRange>(*function_call);
  return absl::OkStatus();
}

}

