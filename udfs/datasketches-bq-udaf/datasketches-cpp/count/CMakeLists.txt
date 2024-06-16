# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_library(count INTERFACE)

add_library(${PROJECT_NAME}::COUNT ALIAS count)

if (BUILD_TESTS)
    add_subdirectory(test)
endif()

target_include_directories(count
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
        )

target_link_libraries(count INTERFACE common)

install(TARGETS count
        EXPORT ${PROJECT_NAME}
        )

install(FILES
        include/count_min.hpp
        include/count_min_impl.hpp
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")
