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

#ifndef ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
#define ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_

#include "zetasql_helper/local_service/local_service.grpc.pb.h"
#include "zetasql_helper/local_service/local_service.pb.h"
#include "zetasql_helper/local_service/local_service.h"

namespace bigquery::utils::zetasql_helper::local_service {

// Implementation of ZetaSql Helper LocalService Grpc service.
class ZetaSqlHelperLocalServiceGrpcImpl : public ZetaSqlHelperLocalService::Service {
 public:

  // A dummy method right now. It will be converted to a health check in future.
  grpc::Status Hello(grpc::ServerContext *context, const HelloRequest *request, HelloResponse *response) override;

  grpc::Status Tokenize(grpc::ServerContext *context, const TokenizeRequest *req,
                        TokenizeResponse *resp) override;

 private:
  ZetaSqlHelperLocalServiceImpl service_;
};

}  // bigquery::utils::zetasql_helper::local_service

#endif  // ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
