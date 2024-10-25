import asyncio
from playwright.async_api import async_playwright

# https://playwright.dev/python/docs/library#interactive-mode-repl

async def print_title(headless:bool = True):
    async with async_playwright() as p:
        for browser_type in [p.chromium, p.firefox, p.webkit]:
            browser = await browser_type.launch(headless=headless)
            page = await browser.new_page()
            await page.goto("http://playwright.dev")
            print(f'El navegador es: {browser_type.name}')
            print(await page.title())
            await browser.close()
            # p.stop()

async def test_siif():
    # async plywright with context manager
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=False,
                args=["--start-maximized"]
            )
            # initialize a new context for the browser to get maximized window and manage several pages
            context = await browser.new_context(no_viewport=True)
            home_page = await context.new_page()
            # Login Page
            await home_page.goto('https://siif.cgpc.gob.ar/mainSiif/faces/login.jspx')
            await home_page.locator('id=pt1:it1::content').fill('27corralesfs')
            await home_page.locator('id=pt1:it2::content').fill('fsc188')
            await home_page.locator('id=pt1:cb1').click()
            await home_page.wait_for_load_state('networkidle')
            btn_reports = home_page.locator('id=pt1:cb12')
            await btn_reports.wait_for()
            await btn_reports.click()
            await home_page.wait_for_load_state('networkidle')
            # New Tab generated
            async with context.expect_page() as new_page_info:
                btn_ver_reportes = home_page.locator('id=pt1:cb14')
                await btn_ver_reportes.wait_for()
                await btn_ver_reportes.click() # Opens a new tab
            reports_page = await new_page_info.value
            all_pages = context.pages
            print(f'La lista de paginas es: {all_pages}')
        except Exception as e:
            print(f"Ocurrio un error: {e}")
        finally:
            try:
                await home_page.locator('id=pt1:pt_np1:pt_cni1').click()
            except Exception as e:
                print(f"Ocurrio un error: {e}")
            await context.close()
            await browser.close()
        # p.stop()

async def main():
    # await print_title(headless=False)
    await test_siif()

# --------------------------------------------------
if __name__ == '__main__':
    asyncio.run(main())
    # From invicodatpy/src
    # python -m invicodatpy.siif.playwright_async