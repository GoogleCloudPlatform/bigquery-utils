#!/usr/bin/env bash
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This software is provided as-is,
# without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
TFLINT_VERSION="v0.20.1"
TFLINT_BASE_URL="https://github.com/terraform-linters/tflint/releases/download"
TFLINT_ZIP="tflint_$(uname | tr '[:upper:]' '[:lower:]')_amd64.zip"
echo "Downloading from ${TFLINT_BASE_URL}/${TFLINT_VERSION}/${TFLINT_ZIP}"
curl -Lo /tmp/tflint.zip "${TFLINT_BASE_URL}/${TFLINT_VERSION}/${TFLINT_ZIP}"
sudo unzip /tmp/tflint.zip -d /bin
