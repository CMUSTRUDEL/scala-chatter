from requests_oauthlib import OAuth1, OAuth2
import requests
import base64
import json
import socket
import os
import sys

class API_Client(object):

    def __init__(self, client_id, cons_key, cons_secret, acc_key, acc_secret):
        self.client_id = client_id
        self.cons_key = cons_key
        self.cons_secret = cons_secret
        self.acc_key = acc_key
        self.acc_secret = acc_secret

        bearer_token = base64.b64encode('{}:{}'.format(cons_key, cons_secret).encode('utf8'))
        post_headers = {
            'Authorization': 'Basic {0}'.format(bearer_token.decode('utf8')),
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
        }
        res = requests.post(url='https://api.twitter.com/oauth2/token',
                                     data={'grant_type':'client_credentials'},
                                     headers = post_headers)
        bearer_creds =  res.json()

        oauth = OAuth1(cons_key, cons_secret, acc_key, acc_secret)
        self.oauth_token = oauth

    def get_request(self, url, http_method="GET", post_body=None, http_headers=None):
        resp = requests.get(url, auth=self.oauth_token)
        return resp.content, resp.status_code

    def put_request(self, url, http_method="PUT", post_body=None, http_headers=None):
        resp = requests.put(url, auth=self.oauth_token)
        return resp.content, resp.status_code

        '''
        content = self.client.request(
            url,
            method=http_method,
            body=post_body,
            headers=http_headers,
        )

        return content, resp.status
        '''

    def get_remaining_hits(self, resource, resource_key):
        remaining_hits = 0
        try:
            resp = 400
            while (resp != 200):
                url = 'https://api.twitter.com/1.1/application/rate_limit_status.json?resources=' + str(resource)
                ratereq, resp = self.get_request(url)
                rls = json.loads(ratereq)

            remaining_hits = rls['resources'][resource][resource_key]['remaining'];
            print ('remaining hits' + str(remaining_hits))
        except Exception as e:
            print("Exception while doing this." + str(e))

        return remaining_hits


def load_clients(json_arr):
    client_arr = []
    for i in range(0, len(json_arr)):
        client_id = i
        element = json_arr[i]
        cons_key = element['consumer_key']
        cons_sec = element['consumer_secret']
        acc_key = element['access_key']
        acc_secret = element['access_secret']

        client = API_Client(client_id, cons_key, cons_sec, acc_key, acc_secret)
        client_arr.append(client)
        print("Client:" + str(client_id) + " Loaded.")
    return client_arr


def shift_clients(curr_i, arr):
    curr_i = (curr_i + 1) % len(arr)
    return curr_i, arr[curr_i]