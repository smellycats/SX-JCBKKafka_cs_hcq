# -*- coding: utf-8 -*-
import json

import requests


class JCBK(object):

    def __init__(self, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.path = kwargs['path']

        self.headers = {
            'content-type': 'application/json',
            'apikey': kwargs['apikey']
        }
        self.status = False

    def __del__(self):
        pass
        
    def post_jcbk(self, data):
        url = 'http://{0}:{1}{2}/jcbk'.format(self.host, self.port, self.path)
        try:
            r = requests.post(url, headers=self.headers, data=json.dumps(data), timeout=10)
            return r
            #if r.status_code == 201:
            #    return json.loads(r.text)
            #else:
            #    self.status = False
            #    raise Exception('url: {url}, status: {code}, {text}'.format(
            #        url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def post_jcbk_list(self, data):
        url = 'http://{0}:{1}{2}/jcbk_list'.format(self.host, self.port, self.path)
        try:
            r = requests.post(url, headers=self.headers, data=json.dumps(data))
            if r.status_code == 201:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise

    def get_root(self):
        url = 'http://{0}:{1}{2}'.format(self.host, self.port, self.path)
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return json.loads(r.text)
            else:
                self.status = False
                raise Exception('url: {url}, status: {code}, {text}'.format(
                    url=url, code=r.status_code, text=r.text))
        except Exception as e:
            self.status = False
            raise
