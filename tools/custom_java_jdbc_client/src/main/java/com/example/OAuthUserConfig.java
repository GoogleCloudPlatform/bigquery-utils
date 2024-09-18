/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.util.HashMap;
import java.util.Objects;

public class OAuthUserConfig {

    // required params
    private final OAuthUserType userType;
    private final String projectId;

    // optional params
    private final String serviceAccountEmail;
    private final String keyFilePath;
    private final String accessToken;
    private final String refreshToken;

    private OAuthUserConfig(Builder builder) {
        this.userType = builder.userType;
        this.projectId = builder.projectId;
        this.serviceAccountEmail = builder.serviceAccountEmail;
        this.keyFilePath = builder.keyFilePath;
        this.accessToken = builder.accessToken;
        this.refreshToken = builder.refreshToken;
    }

    public HashMap<String,String> getOAuthUserConfigMap() {
        HashMap<String,String> oAuthUserConfigMap = new HashMap<>();
        oAuthUserConfigMap.put("OAuthType", this.userType.typeValue.toString());
        oAuthUserConfigMap.put("ProjectId", this.projectId);

        switch (this.userType) {
            case USER:
            case APPLICATION_DEFAULT_CREDENTIALS:
                break;
            case SERVICE_ACCOUNT:
                oAuthUserConfigMap.put("OAuthServiceAcctEmail", this.serviceAccountEmail);
                oAuthUserConfigMap.put("OAuthPvtKeyPath", this.keyFilePath);
                break;
            case TOKEN:
                oAuthUserConfigMap.put("OAuthAccessToken", this.accessToken);
                oAuthUserConfigMap.put("OAuthRefreshToken", this.refreshToken);
                break;
            case EXTERNAL_ACCOUNT:
                oAuthUserConfigMap.put("OAuthPvtKeyPath", this.keyFilePath);
                break;
        }

        return oAuthUserConfigMap;
    }

// Builder class
    public static class Builder {
        // required params
        private final OAuthUserType userType;
        private final String projectId;

        // optional params
        private String serviceAccountEmail;
        private String keyFilePath;
        private String accessToken;
        private String refreshToken;

        public Builder(OAuthUserType userType, String projectId) {
            Objects.requireNonNull(userType, "userType cannot be null.");
            Objects.requireNonNull(projectId, "projectId cannot be null.");
            this.userType=userType;
            this.projectId=projectId;
        }

    public Builder setServiceAccountEmail(String serviceAccountEmail) {
        this.serviceAccountEmail = serviceAccountEmail;
        return this;
    }

    public Builder setKeyFilePath(String keyFilePath) {
        this.keyFilePath = keyFilePath;
        return this;
    }

    public Builder setAccessToken(String accessToken) {
        this.accessToken = accessToken;
        return this;
    }

    public Builder setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
        return this;
    }

    public OAuthUserConfig build() {
        OAuthUserConfig oAuthUserConfig = new OAuthUserConfig(this);
        validateOAuthUserConfigObject(oAuthUserConfig);
        return oAuthUserConfig;
        }

    private void validateOAuthUserConfigObject(OAuthUserConfig oAuthUserConfig) {
            // Validate that required fields are provided for given user type.
            switch (oAuthUserConfig.userType) {
                case USER:
                case APPLICATION_DEFAULT_CREDENTIALS:
                    break;
                case SERVICE_ACCOUNT:
                    Objects.requireNonNull(serviceAccountEmail, "serviceAccountEmail cannot be null.");
                    Objects.requireNonNull(keyFilePath, "Service account keyFilePath cannot be null.");
                    break;
                case TOKEN:
                    if (accessToken == null && refreshToken == null) {
                        throw new NullPointerException("At least one of accessToken or refreshToken must be non-null.");
                    }
                    break;
                case EXTERNAL_ACCOUNT:
                    Objects.requireNonNull(keyFilePath, "External account keyFilePath cannot be null.");
                    break;
                }
            }
    }
}
