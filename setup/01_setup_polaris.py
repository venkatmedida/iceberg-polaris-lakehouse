#!/usr/bin/env python3
"""
Bootstrap Polaris: create catalog (backed by ADLS), namespace, service principal,
roles, and grants so Spark can read/write Iceberg tables.

Run once after `make up`:
    python setup/01_setup_polaris.py

The POLARIS_CLIENT_ID / POLARIS_CLIENT_SECRET printed on first run must be
saved to .env before running Spark jobs.
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# ── connection params ─────────────────────────────────────────────
_base = os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog").rstrip("/")
CATALOG_API = _base                                        # /api/catalog
MGMT_API    = _base.replace("/api/catalog", "/api/management/v1")

ROOT_CLIENT_ID     = os.environ["POLARIS_ROOT_CLIENT_ID"]
ROOT_CLIENT_SECRET = os.environ["POLARIS_ROOT_CLIENT_SECRET"]

CATALOG_NAME   = os.environ["POLARIS_CATALOG_NAME"]
NAMESPACE      = os.environ["POLARIS_NAMESPACE"]
AZURE_TENANT   = os.environ["AZURE_TENANT_ID"]
STORAGE_ACCT   = os.environ["AZURE_STORAGE_ACCOUNT"]
CONTAINER      = os.environ["AZURE_CONTAINER"]

# ── Polaris object names ──────────────────────────────────────────
SPARK_PRINCIPAL   = "spark_principal"
SPARK_PRIN_ROLE   = "spark_principal_role"
CATALOG_ROLE      = "catalog_admin_role"

BASE_LOCATION = (
    f"abfss://{CONTAINER}@{STORAGE_ACCT}.dfs.core.windows.net/warehouse"
)


# ─────────────────────────────────────────────────────────────────
def get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        f"{CATALOG_API}/v1/oauth/tokens",
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "PRINCIPAL_ROLE:ALL",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def api(method: str, path: str, headers: dict, **kwargs) -> requests.Response:
    url = path if path.startswith("http") else f"{MGMT_API}{path}"
    resp = getattr(requests, method)(url, headers=headers, timeout=15, **kwargs)
    if resp.status_code not in (200, 201, 409):
        print(f"  ERROR {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    return resp


def catalog_api(method: str, path: str, headers: dict, **kwargs) -> requests.Response:
    url = f"{CATALOG_API}{path}"
    resp = getattr(requests, method)(url, headers=headers, timeout=15, **kwargs)
    if resp.status_code not in (200, 201, 409):
        print(f"  ERROR {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    return resp


# ─────────────────────────────────────────────────────────────────
def main() -> None:
    print("Obtaining root token …")
    token   = get_token(ROOT_CLIENT_ID, ROOT_CLIENT_SECRET)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 1. Create catalog
    print(f"\n[1/7] Creating catalog '{CATALOG_NAME}' …")
    r = api("post", "/catalogs", headers, json={
        "catalog": {
            "name":       CATALOG_NAME,
            "type":       "INTERNAL",
            "properties": {"default-base-location": BASE_LOCATION},
            "storageConfigInfo": {
                "storageType":      "AZURE",
                "tenantId":         AZURE_TENANT,
                "allowedLocations": [
                    f"abfss://{CONTAINER}@{STORAGE_ACCT}.dfs.core.windows.net/"
                ],
            },
        }
    })
    print(f"     {r.status_code} — base location: {BASE_LOCATION}")

    # 2. Create namespace
    print(f"\n[2/7] Creating namespace '{NAMESPACE}' …")
    r = catalog_api("post", f"/v1/{CATALOG_NAME}/namespaces", headers,
                    json={"namespace": [NAMESPACE], "properties": {}})
    print(f"     {r.status_code}")

    # 3. Create service principal
    print(f"\n[3/7] Creating principal '{SPARK_PRINCIPAL}' …")
    r = api("post", "/principals", headers,
            json={"principal": {"name": SPARK_PRINCIPAL, "type": "SERVICE"}})
    if r.status_code == 201:
        creds = r.json().get("credentials", {})
        client_id     = creds.get("clientId", "")
        client_secret = creds.get("clientSecret", "")
        print(f"\n  *** SAVE THESE TO .env ***")
        print(f"  POLARIS_CLIENT_ID={client_id}")
        print(f"  POLARIS_CLIENT_SECRET={client_secret}")
        print()
    else:
        print(f"     {r.status_code} — already exists, credentials unchanged")

    # 4. Create principal role
    print(f"\n[4/7] Creating principal role '{SPARK_PRIN_ROLE}' …")
    r = api("post", "/principal-roles", headers,
            json={"principalRole": {"name": SPARK_PRIN_ROLE}})
    print(f"     {r.status_code}")

    # 5. Assign principal → role
    print(f"\n[5/7] Assigning role to principal …")
    r = api("put", f"/principals/{SPARK_PRINCIPAL}/principal-roles", headers,
            json={"principalRole": {"name": SPARK_PRIN_ROLE}})
    print(f"     {r.status_code}")

    # 6. Create catalog role and grant privileges
    print(f"\n[6/7] Creating catalog role '{CATALOG_ROLE}' and granting privileges …")
    api("post", f"/catalogs/{CATALOG_NAME}/catalog-roles", headers,
        json={"catalogRole": {"name": CATALOG_ROLE}})

    for priv in [
        "CATALOG_MANAGE_CONTENT",   # create/drop tables & namespaces
        "TABLE_WRITE_DATA",
        "TABLE_READ_DATA",
    ]:
        r = api("put",
                f"/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE}/grants",
                headers,
                json={"grant": {"type": "catalog", "privilege": priv}})
        print(f"     granted {priv}: {r.status_code}")

    # 7. Link catalog role → principal role
    print(f"\n[7/7] Linking catalog role to principal role …")
    r = api("put",
            f"/principal-roles/{SPARK_PRIN_ROLE}/catalog-roles/{CATALOG_NAME}",
            headers,
            json={"catalogRole": {"name": CATALOG_ROLE}})
    print(f"     {r.status_code}")

    print(f"\nPolaris setup complete!")
    print(f"  REST catalog : {CATALOG_API}")
    print(f"  Catalog      : {CATALOG_NAME}")
    print(f"  Namespace    : {CATALOG_NAME}.{NAMESPACE}")
    print(f"  Storage      : {BASE_LOCATION}")


if __name__ == "__main__":
    main()
