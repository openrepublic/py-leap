# py-leap: Antelope protocol framework
# Copyright 2021-eternity Guillermo Rodriguez

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import httpx


class HyperionAPI:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

        self.headers = {"User-Agent": "HyperionClient/1.0"}

        self._client = httpx.Client(
            timeout=httpx.Timeout(5.0),
            headers=self.headers,
        )

        self._async_client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0),
            headers=self.headers,
        )

    def get_actions(self, **kwargs):
        response = self._client.get(
            f"{self.endpoint}/v2/history/get_actions",
            params=kwargs
        )
        response.raise_for_status()
        return response.json()

    async def aget_actions(self, **kwargs):
        response = await self._async_client.get(
            f"{self.endpoint}/v2/history/get_actions",
            params=kwargs
        )
        response.raise_for_status()
        return response.json()
