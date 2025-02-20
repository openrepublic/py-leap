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
