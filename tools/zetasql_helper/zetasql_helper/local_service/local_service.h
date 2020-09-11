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

#ifndef ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_
#define ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_

#include "zetasql_helper/local_service/local_service.pb.h"
#include "absl/status/status.h"

namespace bigquery::utils::zetasql_helper::local_service {

class ZetaSqlHelperLocalServiceImpl {
 public:
  ZetaSqlHelperLocalServiceImpl(const ZetaSqlHelperLocalServiceImpl &) = delete;
  ZetaSqlHelperLocalServiceImpl &operator=(const ZetaSqlHelperLocalServiceImpl &) = delete;

  absl::Status Tokenize(const TokenizeRequest& req,
                        TokenizeResponse* resp);

  absl::Status ExtractFunctionRange(const ExtractFunctionRangeRequest& request,
                                    ExtractFunctionRangeResponse* response);

  absl::Status LocateTableRanges(const LocateTableRangesRequest& request,
                                 LocateTableRangesResponse* response);

  absl::Status GetAllKeywords(const GetAllKeywordsRequest&request,
                              GetAllKeywordsResponse* response);

  absl::Status FixColumnNotGrouped(const FixColumnNotGroupedRequest& request,
                                   FixColumnNotGroupedResponse* response);

  absl::Status FixDuplicateColumns(const FixDuplicateColumnsRequest& request,
                                   FixDuplicateColumnsResponse* response);

  ZetaSqlHelperLocalServiceImpl() = default;
};

}

#endif //ZETASQL_HELPER_ZETASQL_HELPER_LOCAL_SERVICE_LOCAL_SERVICE_H_
