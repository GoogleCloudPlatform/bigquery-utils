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
set -eao pipefail

TF_DOCS_VERSION="v0.9.1"
TF_DOCS_BASE_URL="https://github.com/terraform-docs/terraform-docs/releases/download/"
TF_DOCS_BIN="terraform-docs-${TF_DOCS_VERSION}-$(uname | tr '[:upper:]' '[:lower:]')-amd64"
echo "Downloading from ${TF_DOCS_BASE_URL}/${TF_DOCS_VERSION}/${TF_DOCS_BIN}"
curl -Lo ./terraform-docs "${TF_DOCS_BASE_URL}/${TF_DOCS_VERSION}/${TF_DOCS_BIN}"
chmod +x ./terraform-docs
sudo mv ./terraform-docs /bin
