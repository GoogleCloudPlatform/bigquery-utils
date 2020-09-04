//
// Created by mepan on 8/22/20.
//

#ifndef ZETASQL_HELPER_ZETASQL_HELPER_FUNCTION_LOCATE_FUNCTION_H_
#define ZETASQL_HELPER_ZETASQL_HELPER_FUNCTION_LOCATE_FUNCTION_H_
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

#include "zetasql/public/parse_location.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql_helper/scanner/function_range.pb.h"

namespace bigquery::utils::zetasql_helper {

// A struct to store the range of different components of a function. It includes the range
// of the whole function, the name, and each arguments.
struct FunctionRange {
  zetasql::ParseLocationRange function;
  zetasql::ParseLocationRange name;
  std::vector<zetasql::ParseLocationRange> arguments;

  FunctionRange(const zetasql::ASTFunctionCall& function_call);

  // Serialize the FunctionRange to its Protobuf object.
  zetasql_base::StatusOr<FunctionRangeProto> ToProto() const;

};

// Extract the FunctionRange of a function beginning at the input row and column number.
// Both row and column number are 1-based index. The extracted FunctionRange will be
// assigned to the output unique pointer. If an error occurs, a status with the error
// will be returned. Otherwise, a OKStatus will be returned.
//
// Extraction example:
// foo.bar(123, foo.bar(1,2,3), "a") will be extracted as
// function: foo.bar(123, foo.bar(1,2,3), "a")
// name: foo.bar
// arguments: [123, foo.bar(1,2,3), "a"]
absl::Status ExtractFunctionRange(absl::string_view query,
                                  int row,
                                  int column,
                                  std::unique_ptr<FunctionRange>* output);

}

#endif //ZETASQL_HELPER_ZETASQL_HELPER_FUNCTION_LOCATE_FUNCTION_H_
