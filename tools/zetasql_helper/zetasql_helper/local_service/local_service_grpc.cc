#include "zetasql_helper/local_service/local_service_grpc.h"

namespace bigquery::utils::zetasql_helper::local_service {
using namespace bigquery::utils::zetasql_helper;


grpc::Status ZetaSqlHelperLocalServiceGrpcImpl::Hello(grpc::ServerContext *context,
                                                      const HelloRequest *request,
                                                      HelloResponse *response) {
  auto greeting = "Hello, " + request->name();
  response->set_greeting(greeting);
  return grpc::Status();
}
}
