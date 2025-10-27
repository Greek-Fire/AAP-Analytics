from dataclasses import dataclass
import asyncio, aiohttp, requests
from typing import AsyncIterator, Dict, List, Optional, Iterator
from urllib.parse import urlencode

@dataclass
class AAPConfig:
    base_url: str
    verify_ssl: bool
    token: str

class AAPClient:
    """Pulls job metadata pages *synchronously*; downloads stdout *concurrently*."""

    JOB_PAGE = "/api/v2/jobs/"
    STDOUT_TPL = "/api/v2/jobs/{id}/stdout/?format=txt&start_line=-100"

    def __init__(self, cfg: AAPConfig, page_size: int = 100):
        self.cfg = cfg
        self.page_size = page_size
        self._headers = {
            "Authorization": f"Bearer {cfg.token}",
            "Accept-Encoding": "gzip"
        }

    def iter_jobs(
        self,
        start_id: Optional[int] = None,
        end_id:   Optional[int] = None,
    ) -> Iterator[Dict]:
        """
        Yield job dictionaries, letting the server perform id filtering.

        * --start-id  →  id__gte (≥  start)
        * --end-id    →  id__lte (≤  end)

        If either flag is omitted its filter key is *not* included, so the
        request behaves exactly like the user expects.
        """
        
        query: Dict[str, str] = {"page_size": str(self.page_size)}

        if start_id is not None:
            query["id__gte"] = str(start_id)     # ≥  start‑id
        if end_id is not None:
            query["id__lte"] = str(end_id)       # ≤  end‑id

        page = "{}?{}".format(self.JOB_PAGE, urlencode(query))
        session = requests.Session()
        session.headers.update(self._headers)
        session.verify = self.cfg.verify_ssl

        while page:
            resp = session.get(self.cfg.base_url + page)
            resp.raise_for_status()
            payload = resp.json()

            # Every job in results already satisfies the requested range
            for job in payload["results"]:
                yield job

            page = payload["next"]               # pagination (rarely needed)


    async def get_stdout_bulk(self, job_ids: List[int]) -> Dict[int, str]:
        """Returns {job_id: stdout_txt} for many jobs concurrently."""
        url = self.cfg.base_url + self.STDOUT_TPL
        conn = aiohttp.TCPConnector(ssl=self.cfg.verify_ssl)
        async with aiohttp.ClientSession(
            connector=conn, headers=self._headers
        ) as session:

            async def _fetch(jid):
                async with session.get(url.format(id=jid)) as r:
                    if r.status == 200:
                        return jid, await r.text()
                    return jid, ""

            coros = [_fetch(j) for j in job_ids]
            return dict(await asyncio.gather(*coros))
