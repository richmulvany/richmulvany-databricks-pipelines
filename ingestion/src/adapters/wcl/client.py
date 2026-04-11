"""WarcraftLogs v2 GraphQL API adapter."""

import os
from typing import Any

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.src.adapters.base import BaseAdapter

log = structlog.get_logger(__name__)

# WarcraftLogs v2 uses OAuth2 client credentials
TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
API_URL = "https://www.warcraftlogs.com/api/v2/client"


class WarcraftLogsAdapter(BaseAdapter):
    """
    Adapter for the WarcraftLogs v2 GraphQL API.

    Authentication:
        Requires WARCRAFTLOGS_CLIENT_ID and WARCRAFTLOGS_CLIENT_SECRET
        environment variables (or Databricks secret scope equivalents).

    Rate limits:
        WCL uses a point-based system. Each query costs points; the budget
        resets hourly. Conservative defaults are set below.
    """

    def __init__(self) -> None:
        self._client_id = os.environ["SOURCE_API_CLIENT_ID"]
        self._client_secret = os.environ["SOURCE_API_CLIENT_SECRET"]
        self._access_token: str | None = None
        self._http: httpx.AsyncClient | None = None

    async def authenticate(self) -> None:
        """Obtain an OAuth2 bearer token via client credentials flow."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
            )
            response.raise_for_status()
            self._access_token = response.json()["access_token"]
            log.info("wcl.authenticated")

        self._http = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self._access_token}"},
            timeout=30.0,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
    )
    async def fetch_raw(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a GraphQL query against the WCL API.

        Args:
            endpoint: Ignored for GraphQL — pass any descriptive label.
            params: Must contain 'query' (str) and optionally 'variables' (dict).

        Returns:
            The 'data' field from the GraphQL response.
        """
        if self._http is None:
            raise RuntimeError("Call authenticate() before fetch_raw()")

        query = params.get("query", "")
        variables = params.get("variables", {})

        log.debug("wcl.query", endpoint=endpoint, variables=variables)

        response = await self._http.post(
            API_URL,
            json={"query": query, "variables": variables},
        )
        response.raise_for_status()

        payload = response.json()
        if "errors" in payload:
            log.error("wcl.graphql_errors", errors=payload["errors"])
            raise ValueError(f"GraphQL errors: {payload['errors']}")

        return payload.get("data", {})

    def get_source_name(self) -> str:
        return "wcl"

    def get_rate_limit_config(self) -> dict[str, int]:
        return {
            "requests_per_minute": 30,
            "requests_per_hour": 300,
        }

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http:
            await self._http.aclose()

    # ── Convenience query methods ─────────────────────────────────────────────

    async def fetch_guild_reports(
        self, guild_name: str, server_slug: str, server_region: str
    ) -> dict[str, Any]:
        """Fetch recent raid reports for a guild."""
        query = """
        query GuildReports($guildName: String!, $serverSlug: String!, $serverRegion: String!) {
          guildData {
            guild(name: $guildName, serverSlug: $serverSlug, serverRegion: $serverRegion) {
              id
              name
              server { slug region }
              attendance(limit: 16) {
                data {
                  code startTime title zone { name }
                }
              }
            }
          }
        }
        """
        return await self.fetch_raw(
            "guild_reports",
            {
                "query": query,
                "variables": {
                    "guildName": guild_name,
                    "serverSlug": server_slug,
                    "serverRegion": server_region,
                },
            },
        )

    async def fetch_report_fights(self, report_code: str) -> dict[str, Any]:
        """Fetch fight breakdown for a specific report."""
        query = """
        query ReportFights($code: String!) {
          reportData {
            report(code: $code) {
              code
              title
              startTime
              endTime
              fights(killType: Encounters) {
                id
                name
                kill
                startTime
                endTime
                difficulty
                fightPercentage
                bossPercentage
              }
            }
          }
        }
        """
        return await self.fetch_raw(
            "report_fights",
            {"query": query, "variables": {"code": report_code}},
        )
