import requests
import json
from cogschemas import Cogmeta
import pprint
import logging
from marshmallow import EXCLUDE
from requests_oauthlib import OAuth2Session
import os 
import sys 
from majora_endpoints import ENDPOINTS
from datetime import datetime 

def oauth_load_tokens():
    config_path = ".cogsub-tokens"
    if os.path.exists(config_path):
        with open(config_path) as config_fh:
            config = json.load(config_fh)
            return config
    else:
        return {}

def oauth_save_token(token):
    tokens = oauth_load_tokens()
    scope = " ".join(token["scope"])
    tokens[scope] = token

    config_path = ".cogsub-tokens"
    with open(config_path, 'w') as config_fh:
        json.dump(tokens, config_fh)

def oauth_grant_to_token(config, oauth_scope):
    oauth = OAuth2Session(client_id=config["majora_client_id"], redirect_uri=config["majora_server"]+"/o/callback/", scope=oauth_scope)
    print("Please request a grant via:")
    url, state = oauth.authorization_url(config["majora_server"]+"/o/authorize/", approval_prompt="auto")
    print(url)
    authorization_response = ""
    attempt = 1
    while not authorization_response.startswith(config["majora_server"]):
        if attempt == 4:
            print("Giving up on OAuth authentication and aborting. Try again later.\n")
            sys.exit(1)
        elif attempt > 1:
            print("***\nSorry, your response doesn't appear to start with the address of the callback.\nPlease paste the entire URL for the authorization page as seen in your browser bar.\n***\n")
        authorization_response = input('Enter the full callback URL as seen in your browser window\n')
        attempt += 1
    token = oauth.fetch_token(config["majora_server"]+"/o/token/", authorization_response=authorization_response, client_secret=config["majora_client_secret"])
    return oauth, token

def majora_oauth(config, oauth_scope, force_refresh=False):
    tokens = oauth_load_tokens()
    if oauth_scope in tokens:
        # Check that token is valid
        if datetime.fromtimestamp(tokens[oauth_scope]["expires_at"]) <= datetime.now():
            session, token = oauth_grant_to_token(config, oauth_scope)
            oauth_save_token(token)
        else:
            session = OAuth2Session(
                    client_id=config["majora_client_id"],
                    token=tokens[oauth_scope],
                    scope=oauth_scope,
                    auto_refresh_url=config["majora_server"]+"o/token/",
                    auto_refresh_kwargs={
                        "client_id": config["majora_client_id"],
                        "client_secret": config["majora_client_secret"],
                    },
                    token_updater=oauth_save_token,
            )
            if force_refresh:
                #TODO This would actually force a double refresh in the case where the session is update automatically but whatever
                refresh_params = {
                    "client_id": config["majora_client_id"],
                    "client_secret": config["majora_client_secret"],
                }
                token = session.refresh_token(config["majora_server"]+"/o/token/", **refresh_params)
                oauth_save_token(token)

            token = tokens[oauth_scope]
    else:
        # No scoped token
        session, token = oauth_grant_to_token(config, oauth_scope)
        oauth_save_token(token)

    return session, token

def majora_request(data_list, username, config, datapoint, dry = False):
    address = config["majora_server"] + ENDPOINTS[datapoint]['endpoint']
    majora_session, majora_token  = majora_oauth(config, ENDPOINTS[datapoint]['scope'])
    payload = dict(username=username, token="OAUTH", client_name='cogsub', client_version='0.1')
    payload.update(data_list)
    if not dry:
        response = majora_session.post(address,
                headers = {
                    "Content-Type": "application/json",
                    "charset": "UTF-8",
                    "User-Agent": "%s %s" % (payload["client_name"], payload["client_version"]),
                },
                json = payload,
        )

        response_dict = json.loads(response.content)
        if response_dict['errors'] == 0:
            return True
        else:
            logging.error(response_dict['errors'])
            return False
    else:
        logging.debug(pprint.pprint(payload))
        return True