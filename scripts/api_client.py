#!/usr/bin/env python3
"""
Shared Sarvam API client functions.

Provides authentication, file upload, and job status polling used across
paid-file, allocation, and pullback processing scripts.
"""

import httpx
import os
import sys

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

BASE_URL = "https://apps.sarvam.ai"
API_SERVICE_URL = "https://apps.sarvam.ai/api/api-service"
ORG_ID = "axisbank.com"
WORKSPACE_ID = "axisbank-com-defau-3b9581"


async def get_axis_access_token():
    """POST to /api/auth/login with org credentials, return access_token."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{BASE_URL}/api/auth/login",
                json={
                    "org_id": ORG_ID,
                    "user_id": "sarvam-admin@axisbank.com",
                    "password": os.environ["AXIS_API_PASSWORD"],
                },
                timeout=60.0,
            )
            resp.raise_for_status()
            access_token = resp.json().get("access_token")
            print("Access token obtained successfully.")
            return access_token
    except httpx.HTTPStatusError as e:
        print(e)
        raise
    except Exception as e:
        print(e)
        raise


async def upload_user_context_file(file_path: str, access_token: str, workflow_id: str):
    """Upload CSV to workflow via api-service endpoint. Returns job_id."""
    url = f"{API_SERVICE_URL}/orgs/{ORG_ID}/workspaces/{WORKSPACE_ID}/workflows/{workflow_id}/user-context-jobs"
    headers = {"Authorization": f"Bearer {access_token}"}
    with open(file_path, "rb") as f:
        files = {"file": (file_path, f, "text/csv")}
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=headers, files=files, timeout=120.0)
            resp.raise_for_status()
            return resp.json()["job_id"]


async def get_job_status(job_id: str, access_token: str, workflow_id: str):
    """Poll job status via api-service endpoint."""
    url = f"{API_SERVICE_URL}/orgs/{ORG_ID}/workspaces/{WORKSPACE_ID}/workflows/{workflow_id}/jobs/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, timeout=30.0)
        resp.raise_for_status()
        return resp.json()
