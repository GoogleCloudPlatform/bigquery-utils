//
// Created by mepan on 8/22/20.
//

#ifndef ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_
#define ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_

#include "zetasql_helper/local_service/local_service.pb.h"
#include "absl/status/status.h"

namespace bigquery::utils::zetasql_helper::local_service {

class ZetaSqlHelperLocalServiceImpl {
 public:
  ZetaSqlHelperLocalServiceImpl(const ZetaSqlHelperLocalServiceImpl &) = delete;
  ZetaSqlHelperLocalServiceImpl &operator=(const ZetaSqlHelperLocalServiceImpl &) = delete;

  absl::Status Tokenize(const TokenizeRequest *req,
                        TokenizeResponse *resp);

  ZetaSqlHelperLocalServiceImpl() = default;
};

}

#endif //ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_
