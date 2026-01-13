"""Headless Neo4j Browser screenshot via Playwright.

This is intended as a *visual confirmation* aid.
Structural correctness should still be validated via Cypher/pytest.

Usage:
  python scripts/neo4j_browser_screenshot.py \
    --run-id "e2e:enrollment-generated:seed=1337:entities=4" \
    --entity-id enrollment-1 \
    --out neo4j_exports/enrollment-1.png

Requires:
  - Neo4j Browser reachable at http://127.0.0.1:7474
  - Playwright installed + chromium downloaded:
      pip install playwright
      python -m playwright install chromium

Auth defaults match docker-compose:
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=test

Notes:
  Neo4j Browser UI changes across versions; selectors may need tweaks.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


def _neo4j_creds() -> tuple[str, str]:
	user = os.getenv("NEO4J_USER", "neo4j")
	password = os.getenv("NEO4J_PASSWORD", "test")
	return user, password


def _build_query(*, run_id: str, entity_id: str) -> str:
	# Filter hard by run_id and entity_id to avoid mixed graphs.
	return (
		"MATCH p=(a:EnrollmentEvent {entity_id: '"
		+ entity_id
		+ "'})-[:CAUSES {run_id: '"
		+ run_id
		+ "'}]->(b:EnrollmentEvent {entity_id: '"
		+ entity_id
		+ "'}) RETURN p"
	)


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--run-id", required=True)
	parser.add_argument("--entity-id", required=True)
	parser.add_argument("--out", required=True)
	args = parser.parse_args()

	run_id: str = args.run_id
	entity_id: str = args.entity_id
	out = Path(args.out)
	out.parent.mkdir(parents=True, exist_ok=True)

	user, password = _neo4j_creds()
	query = _build_query(run_id=run_id, entity_id=entity_id)

	with sync_playwright() as p:
		browser = p.chromium.launch(headless=True)
		context = browser.new_context(viewport={"width": 1600, "height": 900})
		page = context.new_page()

		# Load Browser landing; this returns JSON on '/' but Browser UI is under /browser/
		page.goto("http://127.0.0.1:7474/browser/", wait_until="domcontentloaded")

		# Attempt to connect.
		# Neo4j Browser versions differ; try common inputs.
		page.wait_for_timeout(500)

		# If there's an auth/connect modal, fill credentials.
		# Try a few selector variants without failing hard.
		for sel in [
			"input[name='username']",
			"input#username",
			"input[placeholder*='Username']",
			"input[aria-label*='Username']",
		]:
			if page.locator(sel).count() > 0:
				page.locator(sel).first.fill(user)
				break

		for sel in [
			"input[name='password']",
			"input#password",
			"input[type='password']",
			"input[placeholder*='Password']",
			"input[aria-label*='Password']",
		]:
			if page.locator(sel).count() > 0:
				page.locator(sel).first.fill(password)
				break

		# Click connect/login if present
		for sel in [
			"button:has-text('Connect')",
			"button:has-text('Log in')",
			"button:has-text('Login')",
			"button:has-text('Continue')",
		]:
			loc = page.locator(sel)
			if loc.count() > 0:
				loc.first.click()
				break

		# Wait for the editor input to be visible.
		# Newer Browser uses Monaco; textarea may be hidden. We use keyboard typing.
		page.wait_for_timeout(1000)

		# Focus the query editor by clicking into the main content area.
		# Try a few known containers.
		focused = False
		for sel in [
			".CodeMirror",
			".monaco-editor",
			"textarea",
			"div[role='textbox']",
			".editor",
			".cypher-editor",
		]:
			loc = page.locator(sel)
			if loc.count() > 0:
				loc.first.click()
				focused = True
				break

		if not focused:
			# Fall back: click somewhere central.
			page.mouse.click(800, 220)

		# Clear editor and type query.
		page.keyboard.press("Meta+A")
		page.keyboard.press("Backspace")
		page.keyboard.type(query)

		# Execute query.
		# Neo4j Browser usually runs on Ctrl/Cmd+Enter.
		page.keyboard.press("Meta+Enter")

		# Wait a bit for graph rendering, then screenshot full page.
		page.wait_for_timeout(2500)
		page.screenshot(path=str(out), full_page=True)

		context.close()
		browser.close()

	print(f"Wrote screenshot: {out}")


if __name__ == "__main__":
	main()
