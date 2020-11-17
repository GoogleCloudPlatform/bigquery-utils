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
FROM python:3.8
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        gnupg2 \
        git \
        nodejs \
        software-properties-common\
        sudo \
        unzip \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && apt-key fingerprint 0EBFCD88 \
    && add-apt-repository \
          "deb [arch=amd64] https://download.docker.com/linux/debian \
          $(lsb_release -cs) \
          stable" \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
         docker-ce docker-ce-cli containerd.io \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SKIP_IN_CONTAINER_CHECK=true

COPY ci/image/in_container/ /root/ci/image/in_container/

RUN /root/ci/image/in_container/install_terraform.sh
RUN /root/ci/image/in_container/install_terraform_docs.sh
RUN /root/ci/image/in_container/install_tflint.sh

COPY requirements-dev.txt /root/

WORKDIR /root
RUN pip install -r requirements-dev.txt --no-cache-dir
