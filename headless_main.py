import asyncio
import re
from playwright.async_api import async_playwright
import csv
import json
from datetime import datetime
import os
from collections import defaultdict
import statistics
import logging
from logging.handlers import RotatingFileHandler

# ----- Persistent Worker Function -----
async def persistent_worker(worker_id: int, total_workers: int, total_pages: int, result_queue: asyncio.Queue, p) -> None:
    """
    Persistent worker that uses its own browser context to scrape pages.
    It starts at page = worker_id (by clicking Next worker_id-1 times initially),
    then after scraping its current page, it clicks the "Next" button
    exactly 'total_workers' times to jump to the next page assigned to this worker.
    The scraped data along with page numbers is put into result_queue.
    """
    logger = logging.getLogger("SKMarketScraper")
    browser = p.chromium
    context = await browser.launch(headless=True)
    ctx = await context.new_context(viewport={"width": 1280, "height": 1024})
    # Block images, stylesheets, and fonts.
    await ctx.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "stylesheet", "font"] else route.continue_())
    page = await ctx.new_page()
    try:
        await page.goto("https://www.sk-ah.com/history", timeout=60000)
        await page.wait_for_load_state("networkidle")
        
        # Navigate to worker's starting page.
        nav_page_selector = "xpath=/html/body/div/main/div[2]/div/div[5]/div/p[1]"
        current_page = 1
        while current_page < worker_id:
            nav_container = await page.query_selector("xpath=/html/body/div/main/div[2]/div/div[5]")
            next_button = await nav_container.query_selector("xpath=.//button[contains(., 'Next')]")
            if not next_button:
                logger.info(f"Worker {worker_id}: Next button not found while initializing.")
                break
            await next_button.click()
            for _ in range(10):
                await asyncio.sleep(0.5)
                text = (await page.eval_on_selector(nav_page_selector, "el => el.textContent")).strip()
                m = re.search(r'Page (\d+)', text)
                if m:
                    new_page = int(m.group(1))
                    if new_page > current_page:
                        current_page = new_page
                        break
        logger.info(f"Worker {worker_id} starting at page {current_page}")
        
        # Process pages assigned to this worker.
        while current_page <= total_pages:
            # Extract data from current page.
            items = await extract_history_items(page)
            # Even if items is empty, put a result so the main loop knows a page was processed.
            await result_queue.put((current_page, items))
            logger.info(f"Worker {worker_id} scraped page {current_page} with {len(items)} items")
            
            # Determine next target page.
            next_target = current_page + total_workers
            if next_target > total_pages:
                logger.info(f"Worker {worker_id}: Next target {next_target} exceeds total_pages {total_pages}. Ending worker.")
                break
            
            # Click Next repeatedly until we reach the desired page.
            while current_page < next_target:
                nav_container = await page.query_selector("xpath=/html/body/div/main/div[2]/div/div[5]")
                next_button = await nav_container.query_selector("xpath=.//button[contains(., 'Next')]")
                if not next_button:
                    logger.info(f"Worker {worker_id}: Next button not found. Ending worker.")
                    return
                is_disabled = await next_button.get_attribute("disabled")
                if is_disabled is not None:
                    logger.info(f"Worker {worker_id}: Next button disabled. Ending worker.")
                    return
                await next_button.click()
                for _ in range(10):
                    await asyncio.sleep(0.5)
                    text = (await page.eval_on_selector(nav_page_selector, "el => el.textContent")).strip()
                    m = re.search(r'Page (\d+)', text)
                    if m:
                        new_page = int(m.group(1))
                        if new_page > current_page:
                            current_page = new_page
                            logger.info(f"Worker {worker_id} navigated to page {current_page}")
                            break
                if current_page < next_target:
                    logger.info(f"Worker {worker_id} expected page {next_target} but got {current_page}. Retrying...")
        logger.info(f"Worker {worker_id} reached end (last page {current_page}).")
    except Exception as e:
        logger.error(f"Worker {worker_id} error: {str(e)}", exc_info=True)
    finally:
        await ctx.close()
        await context.close()

# ----- Extraction Function (shared across workers) -----
async def extract_history_items(page) -> list:
    items = []
    try:
        await page.wait_for_selector("xpath=/html/body/div/main/div[2]/div/div[4]/table", timeout=8000)
        rows = await page.query_selector_all("xpath=/html/body/div/main/div[2]/div/div[4]/table/tbody/tr")
    except Exception:
        return items
    if not rows:
        return items
    for i, row in enumerate(rows):
        try:
            await row.scroll_into_view_if_needed()
            name_element = await row.query_selector("xpath=./td[1]//span[not(ancestor::small)]")
            name_text = await name_element.text_content() if name_element else None
            if not name_text:
                continue
            price_element = await row.query_selector("xpath=./td[2]//div[contains(@class, 'justify-end')]")
            raw_price = await price_element.text_content() if price_element else None
            price_str = re.sub(r"[^\d]", "", raw_price) if raw_price else ""
            if not price_str:
                continue
            price = int(price_str)
            date_element = await row.query_selector("xpath=./td[3]//div[contains(@class, 'justify-end')][1]")
            time_element = await row.query_selector("xpath=./td[3]//small")
            date_text = (await date_element.text_content()).strip() if date_element else ""
            time_text = (await time_element.text_content()).strip() if time_element else ""
            datetime_str = f"{date_text} {time_text}".strip()
            try:
                dt = datetime.strptime(datetime_str, "%m/%d/%Y %I:%M:%S %p")
            except ValueError:
                dt = None
            item = {
                "name": name_text.strip(),
                "price": price,
                "datetime": dt.isoformat() if dt else datetime_str
            }
            items.append(item)
        except Exception:
            continue
    return items

# ----- Main Application for Global Database Handling -----
class SKMarketScraperApp:
    def __init__(self):
        self.data_dir = "sk_market_data"
        self.logger = logging.getLogger("SKMarketScraper")
        self.item_db = defaultdict(list)
    
    def process_history_items(self, items):
        for item in items:
            existing = self.item_db.get(item["name"], [])
            if any(e["timestamp"] == item["datetime"] and e["price"] == item["price"] for e in existing):
                continue
            self.item_db[item["name"]].append({
                "price": item["price"],
                "timestamp": item["datetime"],
                "type": "sale"
            })
    
    def save_item_database(self):
        db_path = os.path.join(self.data_dir, "item_database.json")
        with open(db_path, "w") as f:
            json.dump(self.item_db, f, indent=2)
        self.logger.info(f"Global database saved to {db_path}")
    
    def save_history_snapshot(self, items, batch_number: int = None):
        batch_tag = f"_batch{batch_number}" if batch_number is not None else ""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.data_dir, f"history_snapshot{batch_tag}_{timestamp}.csv")
        try:
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["name", "price", "datetime"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(items)
            self.logger.info(f"History snapshot saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving snapshot: {str(e)}")

# ----- Main Function -----
async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("SKMarketScraper")
    logger.info("Starting persistent parallel scraping")
    
    total_pages = 200  # For testing; set to 5908 when ready.
    max_workers = 20
    batch_size = 40  # Save after every 100 pages scraped.
    
    result_queue = asyncio.Queue()
    
    async with async_playwright() as p:
        # Start persistent workers.
        workers = []
        for worker_id in range(1, max_workers + 1):
            workers.append(asyncio.create_task(persistent_worker(worker_id, max_workers, total_pages, result_queue, p)))
        
        scraped_pages_count = 0
        batch_items = []
        global_items = []
        
        # Consume results until all workers are done and queue is empty.
        while any(not w.done() for w in workers) or not result_queue.empty():
            try:
                page_number, items = await result_queue.get()
                global_items.extend(items)
                batch_items.extend(items)
                scraped_pages_count += 1
                logger.info(f"Main: Received data for page {page_number} (total scraped pages: {scraped_pages_count})")
                if scraped_pages_count % batch_size == 0:
                    app = SKMarketScraperApp()
                    # Merge current global_items.
                    for item in global_items:
                        existing = app.item_db.get(item["name"], [])
                        if not any(e["timestamp"] == item["datetime"] and e["price"] == item["price"] for e in existing):
                            existing.append({
                                "price": item["price"],
                                "timestamp": item["datetime"],
                                "type": "sale"
                            })
                        app.item_db[item["name"]] = existing
                    app.save_item_database()
                    app.save_history_snapshot(batch_items, batch_number=scraped_pages_count // batch_size)
                    logger.info(f"Main: Completed batch of {batch_size} pages; total items so far: {len(global_items)}")
                    batch_items = []
            except Exception as e:
                logger.error(f"Main loop error: {str(e)}", exc_info=True)
                break

        await asyncio.gather(*workers)
    
    logger.info(f"Total pages scraped: {scraped_pages_count}")
    logger.info(f"Total items scraped: {len(global_items)}")
    logger.info("Persistent parallel scraping completed.")

if __name__ == "__main__":
    asyncio.run(main())
