#ifndef ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
#define ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_

#include "zetasql_helper/local_service/local_service.grpc.pb.h"
#include "zetasql_helper/local_service/local_service.pb.h"

namespace bigquery::utils::zetasql_helper::local_service {

// Implementation of ZetaSql Helper LocalService Grpc service.
class ZetaSqlHelperLocalServiceGrpcImpl : public ZetaSqlHelperLocalService::Service {
 public:

  // A dummy method right now. It will be converted to a health check in future.
  grpc::Status Hello(grpc::ServerContext *context, const HelloRequest *request, HelloResponse *response) override;

};

}  // bigquery::utils::zetasql_helper::local_service

#endif  // ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_GRPC_H_
