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

# separate executables for var_opt and ebpps sampling

# VAR OPT SAMPLING
add_executable(var_opt_sampling_test)

target_link_libraries(var_opt_sampling_test sampling common_test_lib)

set_target_properties(var_opt_sampling_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" SAMPLING_TEST_BINARY_PATH)
string(APPEND SAMPLING_TEST_BINARY_PATH "/")
target_compile_definitions(var_opt_sampling_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${SAMPLING_TEST_BINARY_PATH}"
)

add_test(
  NAME var_opt_sampling_test
  COMMAND var_opt_sampling_test
)

target_sources(var_opt_sampling_test
  PRIVATE
  var_opt_sketch_test.cpp
  var_opt_union_test.cpp
  var_opt_allocation_test.cpp
)


# EBPPS SAMPLING
add_executable(ebpps_sampling_test)

target_link_libraries(ebpps_sampling_test sampling common_test_lib)

set_target_properties(ebpps_sampling_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
)

target_compile_definitions(ebpps_sampling_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${SAMPLING_TEST_BINARY_PATH}"
)

add_test(
  NAME ebpps_sampling_test
  COMMAND ebpps_sampling_test
)

target_sources(ebpps_sampling_test
  PRIVATE
  ebpps_sample_test.cpp
  ebpps_sketch_test.cpp
  ebpps_allocation_test.cpp
)


# Compatibility
if (SERDE_COMPAT)
target_sources(var_opt_sampling_test
  PRIVATE
   var_opt_sketch_deserialize_from_java_test.cpp
   var_opt_union_deserialize_from_java_test.cpp
)
endif()

if (GENERATE)
target_sources(var_opt_sampling_test
  PRIVATE
   var_opt_sketch_serialize_for_java.cpp
   var_opt_union_serialize_for_java.cpp
)
endif()
