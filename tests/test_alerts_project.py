"""Spec checks 11-16: users.d file, compose, .env.example, README, integration marker (static)."""

import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from alerts_interface_support import ROOT, readme_section
from dashboards_support import compose_block

USERS_FILE: Path = ROOT / "clickhouse" / "users.d" / "radar_alerts.xml"
VAR: str = "RADAR_ALERTS_CLICKHOUSE_PASSWORD"
INTEGRATION_FILE: str = "test_alerts_interface_integration.py"


def test_11_users_xml() -> None:
    root: ET.Element = ET.parse(USERS_FILE).getroot()
    assert root.tag == "clickhouse"
    readonly: ET.Element | None = root.find("profiles/radar_alerts_readonly/readonly")
    assert readonly is not None
    assert (readonly.text or "").strip() == "1"
    users: ET.Element | None = root.find("users")
    assert users is not None
    assert [u.tag for u in users] == ["radar_alerts"]
    user: ET.Element = users[0]
    password: ET.Element | None = user.find("password")
    assert password is not None
    assert password.attrib == {"from_env": VAR}
    assert (password.text or "").strip() == ""
    profile: ET.Element | None = user.find("profile")
    assert profile is not None
    assert (profile.text or "").strip() == "radar_alerts_readonly"
    ip: ET.Element | None = user.find("networks/ip")
    assert ip is not None
    assert (ip.text or "").strip() == "::/0"
    queries: list[ET.Element] = user.findall("grants/query")
    assert len(queries) == 1
    assert len(user.findall("grants")) == 1
    assert (queries[0].text or "").strip() == "GRANT SELECT ON alerts_v1.*"
    tag: str
    for tag in ("password_sha256_hex", "access_management", "default_database"):
        assert user.find(tag) is None, tag


def test_12_compose_clickhouse_block() -> None:
    block: str = compose_block("clickhouse")
    assert f"{VAR}: ${{{VAR}:?" in block
    assert (
        "- ./clickhouse/users.d/radar_alerts.xml:/etc/clickhouse-server/users.d/radar_alerts.xml:ro" in block
    )
    assert "CLICKHOUSE_USER: radar" in block
    assert "clickhouse-data:/var/lib/clickhouse" in block
    text: str = (ROOT / "compose.yaml").read_text()
    assert text.count(VAR) == block.count(VAR) >= 1


def test_13_new_compose_lines_are_short() -> None:
    lines: list[str] = [
        x for x in (ROOT / "compose.yaml").read_text().splitlines() if VAR in x or "radar_alerts.xml" in x
    ]
    assert len(lines) >= 2
    line: str
    for line in lines:
        assert len(line) <= 110, line


def test_15_readme() -> None:
    text: str = (ROOT / "README.md").read_text()
    storage: re.Match[str] | None = re.search(r"^## Storage", text, re.MULTILINE)
    heading: re.Match[str] | None = re.search(
        r"^## Read-only SQL interface .alerts_v1.\s*$", text, re.MULTILINE
    )
    assert storage is not None
    assert heading is not None
    assert heading.start() > storage.start()
    section: str = readme_section(text, r"^## Read-only SQL interface .alerts_v1.\s*$")
    needle: str
    for needle in [
        "steam_players",
        "steam_chart",
        "bluesky_tag_hourly",
        "DateTime64(6, 'UTC')",
        "LowCardinality(String)",
        "DateTime('UTC')",
        "UInt64",
        "radar_alerts",
        VAR,
        "clickhouse/users.d/radar_alerts.xml",
        "readonly",
        "SQL SECURITY DEFINER",
        "alerts_v2",
        "gamedev-trends_default",
        "radar.observations.v1",
    ]:
        assert needle in section, needle
    status: list[str] = [x for x in text.splitlines() if x.startswith("Status:")]
    assert len(status) == 1
    assert "alerts_v1" in status[0]
    assert "Grafana" in status[0]
    assert "There is no API yet" not in status[0]
    assert "There is no HTTP API yet." in status[0]
    assert "Not covered: the HTTP API." in readme_section(text, r"^## Storage.*$")


def collect(*args: str) -> str:
    result: subprocess.CompletedProcess[str] = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


def test_16_integration_file_is_collected_only_with_the_marker() -> None:
    assert (ROOT / "tests" / INTEGRATION_FILE).exists()
    assert INTEGRATION_FILE in collect("-m", "integration")
    assert INTEGRATION_FILE not in collect()
