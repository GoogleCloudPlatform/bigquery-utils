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

#include "zetasql_helper/local_service/local_service_grpc.h"

namespace {

// A helper function to convert absl::Status to grpc::Status.
grpc::Status ToGrpcStatus(absl::Status status) {
  if (status.ok()) {
    return grpc::Status();
  }
  grpc::StatusCode grpc_code;
  switch (status.code()) {
    case absl::StatusCode::kCancelled:grpc_code = grpc::CANCELLED;
      break;
    case absl::StatusCode::kInvalidArgument:grpc_code = grpc::INVALID_ARGUMENT;
      break;
    case absl::StatusCode::kDeadlineExceeded:grpc_code = grpc::DEADLINE_EXCEEDED;
      break;
    case absl::StatusCode::kNotFound:grpc_code = grpc::NOT_FOUND;
      break;
    case absl::StatusCode::kAlreadyExists:grpc_code = grpc::ALREADY_EXISTS;
      break;
    case absl::StatusCode::kPermissionDenied:grpc_code = grpc::PERMISSION_DENIED;
      break;
    case absl::StatusCode::kResourceExhausted:grpc_code = grpc::RESOURCE_EXHAUSTED;
      break;
    case absl::StatusCode::kFailedPrecondition:grpc_code = grpc::FAILED_PRECONDITION;
      break;
    case absl::StatusCode::kAborted:grpc_code = grpc::ABORTED;
      break;
    case absl::StatusCode::kOutOfRange:grpc_code = grpc::OUT_OF_RANGE;
      break;
    case absl::StatusCode::kUnimplemented:grpc_code = grpc::UNIMPLEMENTED;
      break;
    case absl::StatusCode::kInternal:grpc_code = grpc::INTERNAL;
      break;
    case absl::StatusCode::kUnavailable:grpc_code = grpc::UNAVAILABLE;
      break;
    case absl::StatusCode::kDataLoss:grpc_code = grpc::DATA_LOSS;
      break;
    case absl::StatusCode::kUnauthenticated:grpc_code = grpc::UNAUTHENTICATED;
      break;
    default:grpc_code = grpc::UNKNOWN;
  }
  return grpc::Status(grpc_code, std::string(status.message()), "");
}
}


namespace bigquery::utils::zetasql_helper::local_service {
using namespace bigquery::utils::zetasql_helper;

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::Tokenize(grpc::ServerContext* context, const TokenizeRequest* request,
                                                         TokenizeResponse* response) {
  return ToGrpcStatus(service_.Tokenize(*request, response));
}

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::ExtractFunctionRange(grpc::ServerContext* context,
                                                                     const ExtractFunctionRangeRequest* request,
                                                                     ExtractFunctionRangeResponse* response) {
  return ToGrpcStatus(service_.ExtractFunctionRange(*request, response));
}

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::LocateTableRanges(grpc::ServerContext* context,
                                                                  const LocateTableRangesRequest* request,
                                                                  LocateTableRangesResponse* response) {

  return ToGrpcStatus(service_.LocateTableRanges(*request, response));
}

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::GetAllKeywords(grpc::ServerContext* context,
                                                            const GetAllKeywordsRequest* request,
                                                            GetAllKeywordsResponse* response) {

  return ToGrpcStatus(service_.GetAllKeywords(*request, response));
}

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::FixColumnNotGrouped(grpc::ServerContext* context,
                                                                    const FixColumnNotGroupedRequest* request,
                                                                    FixColumnNotGroupedResponse* response) {

  return ToGrpcStatus(service_.FixColumnNotGrouped(*request, response));
}

grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::FixDuplicateColumns(grpc::ServerContext* context,
                                                                    const FixDuplicateColumnsRequest* request,
                                                                    FixDuplicateColumnsResponse* response) {

  return ToGrpcStatus(service_.FixDuplicateColumns(*request, response));
}

} // bigquery::utils::zetasql_helper::local_service
