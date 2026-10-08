"""Fetch and ScanCode license files for the 2026-03-02 graph case study."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pickle
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm.auto import tqdm

from graph_config_2026_03_02 import RESULTS_DIR

SCANCODE_TOOLKIT_DIR = Path("/home/infres/rapaport/bin/scancode-toolkit")
SCANCODE_BIN = Path(
    os.environ.get(
        "SCANCODE_BIN",
        str(SCANCODE_TOOLKIT_DIR / "venv" / "bin" / "scancode"),
    )
)

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

SWH_API_BASE = "https://archive.softwareheritage.org/api/1"
API_HEADERS = {"Accept": "application/json"}
API_TIMEOUT = 60

cache: dict[str, str | None] = {}
_session: requests.Session | None = None


def _api_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(API_HEADERS)
    return _session


def _resolve_snapshot_revision(snapshot_id: str, branch: str) -> str:
    response = _api_session().get(
        f"{SWH_API_BASE}/snapshot/{snapshot_id}/",
        timeout=API_TIMEOUT,
    )
    response.raise_for_status()
    branches = response.json()["branches"]

    seen: set[str] = set()
    while True:
        if branch in seen:
            raise ValueError(f"snapshot {snapshot_id}: alias loop for branch {branch!r}")
        seen.add(branch)

        branch_info = branches.get(branch)
        if branch_info is None:
            raise ValueError(
                f"snapshot {snapshot_id}: branch {branch!r} not found"
            )

        target_type = branch_info["target_type"]
        if target_type == "alias":
            branch = branch_info["target"]
            continue
        if target_type == "revision":
            return branch_info["target"]

        raise ValueError(
            f"snapshot {snapshot_id}: branch {branch!r} points to {target_type!r}"
        )


def _browse_url_to_api_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    query = parse_qs(parsed.query)

    if "revision" in parts:
        revision_id = parts[parts.index("revision") + 1]
        path = query.get("path", [""])[0]
        return f"{SWH_API_BASE}/revision/{revision_id}/directory/{path}/"

    if "snapshot" in parts:
        snapshot_id = parts[parts.index("snapshot") + 1]
        branch = query.get("branch", ["HEAD"])[0]
        path = query.get("path", [""])[0]
        revision_id = _resolve_snapshot_revision(snapshot_id, branch)
        return f"{SWH_API_BASE}/revision/{revision_id}/directory/{path}/"

    raise ValueError(f"unsupported SWH browse URL: {url}")


def get_raw_file_content(url: str) -> str | None:
    try:
        api_url = _browse_url_to_api_url(url)
        response = _api_session().get(api_url, timeout=API_TIMEOUT)
        if response.status_code == 404:
            print(f"File not found in archive: {url}", file=sys.stderr)
            return None
        response.raise_for_status()

        payload = response.json()
        content = payload.get("content")
        if not content:
            print(f"No file content at path: {url}", file=sys.stderr)
            return None

        data_url = content.get("data_url")
        if not data_url:
            print(f"No raw content URL for: {url}", file=sys.stderr)
            return None

        raw_response = _api_session().get(data_url, timeout=API_TIMEOUT)
        raw_response.raise_for_status()
        return raw_response.text

    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def detect_license(license_text: str | None) -> str | None:
    if not license_text:
        return None

    digest = hashlib.sha1(license_text.encode("utf-8")).hexdigest()
    if digest in cache:
        return cache[digest]

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp:
        temp.write(license_text)
        temp_path = temp.name

    try:
        result = subprocess.run(
            [str(SCANCODE_BIN), "-l", "-n", "30", "--json", "-", temp_path],
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            print(f"License detection failed: {result.stderr}")
            return None

        try:
            json_data = json.loads(result.stdout)

            if "files" in json_data and len(json_data["files"]) > 0:
                matches: dict[tuple[int, int], str] = {}
                max_score = [0, 0]
                for licence in json_data["files"][0]["license_detections"]:
                    for match in licence["matches"]:
                        matches[(match["score"], match["matched_length"])] = match[
                            "license_expression"
                        ]
                        if match["score"] > max_score[0]:
                            max_score[0] = match["score"]
                            max_score[1] = match["matched_length"]
                        elif match["score"] == max_score[0]:
                            max_score[1] = max(max_score[1], match["matched_length"])
                if max_score != [0, 0]:
                    license_scanned = matches[(max_score[0], max_score[1])]
                    cache[digest] = license_scanned
                    return license_scanned
            return None
        except json.JSONDecodeError:
            print("Failed to parse license detection output as JSON")
            return None

    finally:
        os.unlink(temp_path)


def single_url() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and analyze raw file content from a website."
    )
    parser.add_argument("url", help="URL of the website containing the raw file button")
    args = parser.parse_args()

    content = get_raw_file_content(args.url)

    if content:
        print(content)
    else:
        print("Failed to retrieve raw content.", file=sys.stderr)


def main() -> None:
    if not SCANCODE_BIN.is_file():
        raise FileNotFoundError(
            f"ScanCode binary not found at {SCANCODE_BIN}. "
            f"Expected install at {SCANCODE_TOOLKIT_DIR}/venv/bin/scancode"
        )

    results_path = Path(__file__).resolve().parent / RESULTS_DIR
    input_csv = results_path / "repos_modified_path.csv"
    ds = pd.read_csv(input_csv, delimiter=";")
    ds.drop_duplicates().reset_index()

    tqdm.pandas()
    ds["Rev-License-Scanned"] = ds["Rev-License-Path"].progress_apply(
        lambda p: detect_license(get_raw_file_content(p)) if p else None
    )
    with open(results_path / "results_full_part1.pkl", "wb") as f:
        pickle.dump(ds, f)

    ds["Snap-License-Scanned"] = ds["Snap-License-Path"].progress_apply(
        lambda p: detect_license(get_raw_file_content(p)) if p else None
    )
    with open(results_path / "results_full_part2.pkl", "wb") as f:
        pickle.dump(ds, f)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        single_url()
    else:
        main()
