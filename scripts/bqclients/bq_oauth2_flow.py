#!/usr/bin/python3.7

#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""
Authenticating the User by either loading saved credentials from the cache
or by going through the OAuth 2.0 flow in case of a cache miss or invalid cached credentials
"""

from google_auth_oauthlib import flow

import google.oauth2.credentials
import logging
import json
import os
import os.path

__author__ = 'nikunjbhartia@google.com (Nikunj Bhartia)'

_PROJECT = None

# By default: The user credentials received after going through the oauth2 flow
# is stored on disk at ~/.config/bigquery/token.json
_USER_CREDENTIALS_FOLDER = "bigquery"
_USER_CREDENTIALS_FILE = "token.json"
_USER_CREDENTIAL_SCOPE_LIST = ['https://www.googleapis.com/auth/bigquery']

# Client secrets_file.json downlaoded from Google Apis Console
# Ref : https://developers.google.com/api-client-library/dotnet/guide/aaa_client_secrets
#       https://developers.google.com/identity/protocols/oauth2#1.-obtain-oauth-2.0-credentials-from-the-google-api-console.
_CLIENT_SECRETS_FILE = "resources/client_secret.json"

# Provide either the client_secrets_file or the below information
# The client secrets file is used to prevent hardcoding of clientid and client secret in this script
_CLIENT_ID = None
_CLIENT_SECRET = None

Logger = None

def init_logger():
    """Initializing default python Logger"""
    global Logger
    Logger_name = "bqclient"
    Logger = logging.getLogger(Logger_name)
    logging.basicConfig(level=logging.INFO)

def get_user_credentials(
        client_secrets_filepath=None,
        client_id=None,
        client_secret=None,
        scopes=None,
        usercred_dirname=None,
        usercred_filename=None):
    """
     Authenticating the User by either loading saved credentials from the cache
     or by going through the OAuth 2.0 flow in case of a cache miss or invalid cached credentials

     If control goes through an OAuth2 flow, the credentials gets cached in a file on disk in json format

    It will start a local web server to listen for the authorization response of the Oauth2 Flow. Default port: 8080
    Once authorization is complete the authorization server will redirect the user’s browser to the local web server.
    The web server will get the authorization code from the response and shutdown

    Ref: https://cloud.google.com/bigquery/docs/authentication/end-user-installed

    Params:
        client_secrets_filepath: str, optional
            Client secrets filepat
            Either client_secrets_filepath or client_id and client_secret is mandatory
        client_id: str, optional
        client_secret: str, optional
        scopes : list(Str) Comma-separated list of scopes (permissions) to request from Google
            Ref: https://developers.google.com/identity/protocols/googlescopes
        usercred_dirname:
            directory name for storing/reading user credentials
        usercred_filename:
            filename name for storing/reading user credentials

    Return:
       credentials : google.oauth2.credentials.Credentials
            Credentials for the user, with the requested scopes.
    """
    client_config = _load_client_secrets(client_secrets_filepath, client_id, client_secret)
    credentials_filepath = _get_user_credetials_filepath(usercred_dirname, usercred_filename)
    credentials = _load_user_credentials_from_file(credentials_filepath)

    if scopes is None:
        scopes = _USER_CREDENTIAL_SCOPE_LIST
    else:
        scopes = scopes.split(",")

    if credentials is None:
        app_flow = flow.InstalledAppFlow.from_client_config(client_config, scopes=scopes)

        try:
            # Ref: https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html#google_auth_oauthlib.flow.InstalledAppFlow.run_local_server
            credentials = app_flow.run_local_server()

        except Exception as exc:
            Logger.error("Unable to get valid credentials: {}".format(exc))
            raise

    if credentials and not credentials.valid:
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)

    return credentials


def save_user_credentials(
        client_secrets_filepath=None,
        client_id=None,
        client_secret=None,
        scopes=None,
        usercred_dirname=None,
        usercred_filename=None):
    """
    Get and validate credentials cache from local or through Oauth flow.
    Stores the credentials in a local file

    """
    credentials = get_user_credentials(client_secrets_filepath, client_id, client_secret, scopes, usercred_dirname, usercred_filename)
    filepath = _get_user_credetials_filepath(usercred_dirname, usercred_filename)
    _store_user_credentials(credentials, filepath)


def _get_user_credetials_filepath(dirname=None, filename=None):
    """
      Gets the default path for the cached Google user credentials

      Params:
        dirname : str, optional
            directory name for user credentials json file
            Defaults to _USER_CREDENTIALS_FOLDER
        filename : str, optional
            filename for user credentials json file
            Default to _USER_CREDENTIALS_FILE

      Returns:
          str : Path to the Google user credentials json file
          Location : ~/.config/dirname/filename

    """
    if dirname is None:
        dirname = _USER_CREDENTIALS_FOLDER
    if filename is None:
        filename = _USER_CREDENTIALS_FILE

    credentials_path = os.path.join(os.path.expanduser("~"), ".config")
    credentials_path = os.path.join(credentials_path, dirname)
    return os.path.join(credentials_path, filename)


def _load_client_secrets(client_secrets_path=None, client_id=None, client_secret=None):
    """
    Get Client Secrets by loading from a file or from client_id and client_Secret:

    Ref : https://developers.google.com/api-client-library/dotnet/guide/aaa_client_secrets
         https://developers.google.com/identity/protocols/oauth2#1.-obtain-oauth-2.0-credentials-from-the-google-api-console.

    Params :
        client_secrets_path : str, optional
            Path to the client_secrets json file

    Returns :
        json : object with relevant client_secrets informations
    """
    if client_secrets_path is None:
        client_secrets_path = _CLIENT_SECRETS_FILE

    if client_secrets_path:
        with open(client_secrets_path) as f:
            secrets = json.load(f)

        return secrets

    if client_id is None:
        client_id = _CLIENT_ID
    if client_secret is None:
        client_secret = _CLIENT_SECRET

    if client_id and client_secret:
        client_secrets = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_id,
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }
        return client_secrets

    # If none of the above conditions satisfy
    raise ValueError("Client id and Client secret information missing.")


def _load_user_credentials_from_json(credentials_json):
    """
    Creates oauth credentials object from input json object after validating the credential token

    Return :
        credentials : google.oauth2.credentials.Credentials
    """
    credentials = google.oauth2.credentials.Credentials(
        token=credentials_json.get("access_token"),
        refresh_token=credentials_json.get("refresh_token"),
        id_token=credentials_json.get("id_token"),
        token_uri=credentials_json.get("token_uri"),
        client_id=credentials_json.get("client_id"),
        client_secret=credentials_json.get("client_secret"),
        scopes=credentials_json.get("scopes"),
    )

    # Ref:
    #   credential.valid : https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.credentials.html#google.oauth2.credentials.Credentials.valid
    #   credentials.refresh : https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.credentials.html#google.oauth2.credentials.Credentials.refresh

    # Note that we are not storing the access_token in the cache, hence, the refresh function will be used to create an
    # access token with the refresh_token thereby validating the refresh_token as well. If somehow the refresh token was
    # tampered with, the control with go to the Oauth2 flow.
    if credentials and not credentials.valid:
        request = google.auth.transport.requests.Request()
        try:
             credentials.refresh(request)
        except google.auth.exceptions.RefreshError:
            Logger.info("Credentials could be expired or revoked. Try to reauthorize.")
            return None

    return credentials


def _load_user_credentials_from_file(credentials_path):
    """
    Loads user credentials from the local json file and validate the token
    This is used to ensure user doesnt have to go through oauth2 authentication flow on every run by
    checking if there are cached credentials present from previous runs.

    Returns :
        credentials : google.oauth2.credentials.Credentials
            Ref: https://google-auth.readthedocs.io/en/latest/reference/google.oauth2.credentials.html
    """
    try:
        with open(credentials_path) as credentials_file:
            credentials_json = json.load(credentials_file)
    except Exception as exc:
        Logger.debug("Error loading credentials from {}: {}".format(credentials_path, str(exc)))
        return None

    return _load_user_credentials_from_json(credentials_json)


def _store_user_credentials(credentials, filepath):
    """
    Store user credentials information in a local json file.

    Params:
        credentials : google.oauth2.credentials.Credentials
        Filepath: Str
            Filepath to store the credentials on disk
    """
    config_dir = os.path.dirname(filepath)
    if not os.path.exists(config_dir):
        try:
            os.makedirs(config_dir)
        except Exception as exc:
            Logger.warning("Unable to create credentials directory.")
            return

    try:
      with open(filepath, "w") as credentials_file:
        credentials_json = {
            "refresh_token": credentials.refresh_token,
            # "access_token" : credentials.token,
            "id_token": credentials.id_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
            "type": "authorized_user",
        }

        json.dump(credentials_json, credentials_file)

    except IOError:
        Logger.warning("Unable to save credentials.")

if __name__ == '__main__':
    init_logger()

    # picks client secrets from default file and stores user credentials in local default cache location as
    # per global variables
    save_user_credentials()