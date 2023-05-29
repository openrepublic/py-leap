#!/usr/bin/env python3

import sys

from urllib3.util.retry import Retry

import asks
import requests

from requests.adapters import HTTPAdapter


class HyperionAPI:

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

        self._session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=10,
            backoff_factor=0.1,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)

        if 'asks' in sys.modules:
            self._asession = asks.Session(connections=200)

    def _get(self, *args, **kwargs):
        return self._session.get(*args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._session.post(*args, **kwargs)

    async def _aget(self, *args, **kwargs):
        return await self._asession.get(*args, **kwargs)

    async def _apost(self, *args, **kwargs):
        return await self._asession.post(*args, **kwargs)

    def get_actions(self, **kwargs):
        return self._get(
            f'{self.endpoint}/v2/history/get_actions',
            params=kwargs
        ).json()

    async def aget_actions(self, **kwargs):
        return (await self._aget(
            f'{self.endpoint}/v2/history/get_actions',
            params=kwargs
        )).json()
