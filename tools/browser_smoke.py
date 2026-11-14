"""Run against an already-started local simulator; stores browser evidence."""

import asyncio
import json
import time
from pathlib import Path

from playwright.async_api import async_playwright


async def main():
    out = Path("docs/browser")
    out.mkdir(exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path="/usr/bin/chromium", headless=True, args=["--no-sandbox"]
        )
        page = await browser.new_page(viewport={"width": 1512, "height": 1050})
        await page.add_init_script("""const Native = window.WebSocket;
            window.__bitmeshSockets = [];
            window.WebSocket = class extends Native {
                constructor(...args) { super(...args); window.__bitmeshSockets.push(this); }
            };""")
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        await page.goto("http://127.0.0.1:8000")
        await page.get_by_text("Subsystem fleet", exact=True).wait_for()
        await page.get_by_text("s01-n01", exact=True).first.wait_for()
        await page.screenshot(path=str(out / "overview.png"), full_page=True)
        await page.get_by_role("button", name="Subsystems", exact=True).click()
        await page.get_by_role("button", name="Inspect →").nth(1).click()
        await page.get_by_text("Historical trend", exact=True).wait_for()
        await page.screenshot(path=str(out / "som-detail.png"), full_page=True)
        changed = time.monotonic()
        response = await page.request.post(
            "http://127.0.0.1:8000/api/simulation/inject",
            data={"target": "s01-n02", "action": "temperature", "value": 92},
        )
        assert response.ok
        await page.get_by_role("cell", name="92.0 °C", exact=True).wait_for()
        change_to_dom_ms = (time.monotonic() - changed) * 1000
        assert change_to_dom_ms < 1000, f"Fast telemetry exceeded 1 s: {change_to_dom_ms} ms"
        await page.get_by_role("button", name="Control SOM").click()
        await page.get_by_role("button", name="Stop", exact=True).click()
        await page.get_by_role("dialog").wait_for()
        await page.get_by_role("button", name="Cancel", exact=True).click()
        await page.screenshot(path=str(out / "operations.png"), full_page=True)
        await page.get_by_role("button", name="Temperature 92°C").click()
        await page.get_by_role("button", name="Fault console", exact=True).click()
        await page.get_by_text("PS temperature: 92.0 °C (CRITICAL)", exact=True).wait_for()
        await page.screenshot(path=str(out / "fault-console.png"), full_page=True)
        # Browser online/offline lifecycle tests the actual reconnection implementation.
        await page.evaluate("window.__bitmeshSockets.at(-1).close()")
        await page.get_by_text("DISCONNECTED", exact=True).wait_for()
        await page.get_by_text("LIVE", exact=True).wait_for(timeout=15000)
        socket_count = await page.evaluate("window.__bitmeshSockets.length")
        assert socket_count >= 2
        await page.set_viewport_size({"width": 390, "height": 844})
        await page.screenshot(path=str(out / "mobile.png"), full_page=True)
        overflow = await page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
        report = {
            "page_errors": errors,
            "fast_change_to_dom_ms": change_to_dom_ms,
            "websocket_connections": socket_count,
            "reconnected": True,
            "mobile_horizontal_overflow": overflow,
            "verified": [
                "overview",
                "SOM drilldown",
                "fault injection and archive display",
                "offline/reconnect",
                "mobile viewport",
            ],
        }
        (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        await browser.close()
        if errors or overflow:
            raise SystemExit(1)
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
