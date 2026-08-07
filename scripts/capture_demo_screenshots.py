import argparse
import asyncio
import base64
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

import websockets


class ChromePage:
    def __init__(self, websocket_url):
        self.websocket_url = websocket_url
        self.message_id = 0
        self.websocket = None

    async def __aenter__(self):
        self.websocket = await websockets.connect(self.websocket_url, max_size=None)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.websocket.close()

    async def command(self, method, params=None):
        self.message_id += 1
        request_id = self.message_id
        await self.websocket.send(json.dumps({"id": request_id, "method": method, "params": params or {}}))
        while True:
            response = json.loads(await self.websocket.recv())
            if response.get("id") == request_id:
                if "error" in response:
                    raise RuntimeError(response["error"])
                return response.get("result", {})

    async def evaluate(self, expression):
        result = await self.command(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
        )
        return result.get("result", {}).get("value")

    async def wait_for(self, expression, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await self.evaluate(expression):
                return
            await asyncio.sleep(0.5)
        raise TimeoutError(f"Timed out waiting for: {expression}")

    async def screenshot(self, path):
        await self.command("Runtime.evaluate", {"expression": "window.scrollTo(0, 0)"})
        await asyncio.sleep(1)
        result = await self.command("Page.captureScreenshot", {"format": "png", "fromSurface": True})
        Path(path).write_bytes(base64.b64decode(result["data"]))


def create_tab(debug_port, url):
    endpoint = f"http://127.0.0.1:{debug_port}/json/new?{urllib.parse.quote(url, safe=':/')}"
    request = urllib.request.Request(endpoint, method="PUT")
    with urllib.request.urlopen(request) as response:
        return json.load(response)["webSocketDebuggerUrl"]


async def capture(args):
    websocket_url = create_tab(args.debug_port, args.url)
    async with ChromePage(websocket_url) as page:
        await page.command("Page.enable")
        await page.command("Runtime.enable")
        await page.command(
            "Emulation.setDeviceMetricsOverride",
            {
                "width": args.width,
                "height": args.height,
                "deviceScaleFactor": 1,
                "mobile": False,
            },
        )
        await page.command("Page.navigate", {"url": args.url})
        await page.wait_for("document.querySelector('.jiig-incident-hero') !== null")
        await page.wait_for("document.body.innerText.includes('Causal dependency graph')")
        await page.wait_for("document.querySelector('[data-testid=stApp]')?.getAttribute('data-test-script-state') === 'notRunning'")
        await page.screenshot(args.incident_output)

        clicked = await page.evaluate(
            """
            (() => {
              const label = Array.from(document.querySelectorAll('button, label')).find(
                element => element.innerText.includes('Dependency Intelligence')
              );
              if (!label) return false;
              label.click();
              return true;
            })()
            """
        )
        if not clicked:
            raise RuntimeError("Dependency Intelligence navigation was not found")
        await page.wait_for("document.body.innerText.includes('Top dependency hubs')")
        await page.wait_for("document.querySelector('[data-testid=stApp]')?.getAttribute('data-test-script-state') === 'notRunning'")
        await page.screenshot(args.intelligence_output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--debug-port", type=int, default=9222)
    parser.add_argument("--width", type=int, default=1440)
    parser.add_argument("--height", type=int, default=1200)
    parser.add_argument("--incident-output", default="resources/figures/jiig_incident_command.png")
    parser.add_argument("--intelligence-output", default="resources/figures/jiig_dependency_intelligence.png")
    args = parser.parse_args()
    asyncio.run(capture(args))


if __name__ == "__main__":
    main()
