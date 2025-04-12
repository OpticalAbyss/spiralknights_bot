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

class SKMarketScraper:
    def __init__(self):
        self.base_url = "https://www.sk-ah.com/"
        self.history_url = f"{self.base_url}history"
        self.data_dir = "sk_market_data"
        self.logs_dir = "logs"
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        self.setup_logging()
        self.logger = logging.getLogger('SKMarketScraper')
        self.item_db = self.load_item_database()

        # Control flags for pause/stop functionality
        self._pause = False
        self._stop = False
    
    def setup_logging(self):
        logger = logging.getLogger('SKMarketScraper')
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        
        log_file = os.path.join(self.logs_dir, 'sk_market_scraper.log')
        fh = RotatingFileHandler(
            log_file,
            maxBytes=1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        
        logger.addHandler(ch)
        logger.addHandler(fh)
    
    def load_item_database(self):
        db_path = os.path.join(self.data_dir, "item_database.json")
        try:
            if os.path.exists(db_path):
                self.logger.info(f"Loading existing database from {db_path}")
                with open(db_path, 'r') as f:
                    return defaultdict(list, json.load(f))
            self.logger.info("No existing database found, creating new one")
            return defaultdict(list)
        except Exception as e:
            self.logger.error(f"Error loading database: {str(e)}")
            return defaultdict(list)
    
    def save_item_database(self):
        db_path = os.path.join(self.data_dir, "item_database.json")
        try:
            with open(db_path, 'w') as f:
                json.dump(self.item_db, f, indent=2)
            self.logger.info(f"Database successfully saved to {db_path}")
        except Exception as e:
            self.logger.error(f"Error saving database: {str(e)}")
    
    def pause_scraping(self):
        self.logger.info("Scraping paused")
        self._pause = True
    
    def resume_scraping(self):
        self.logger.info("Scraping resumed")
        self._pause = False
    
    def stop_scraping(self):
        self.logger.info("Scraping stop requested")
        self._stop = True

    async def scrape_history_pages(self, max_pages=10):
        async with async_playwright() as p:
            # Enable headless mode (default) and remove slow_mo
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1280, "height": 1024})
            
            # Block images, stylesheets, and fonts to speed up page load.
            await context.route("**/*", lambda route, request: route.abort() if request.resource_type in ["image", "stylesheet", "font"] else route.continue_())

            page = await context.new_page()
            
            try:
                self.logger.info(f"Starting history scraping (max {max_pages} pages)")
                await page.goto(self.history_url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                current_page_num = 1
                all_history_items = []
                
                while current_page_num <= max_pages:
                    self.logger.info(f"Scraping history page {current_page_num}")
                    
                    try:
                        await page.wait_for_selector("xpath=/html/body/div/main/div[2]/div/div[4]/table", timeout=15000)
                        self.logger.debug("History table loaded")
                    except Exception as e:
                        self.logger.warning(f"History table not loaded on page {current_page_num}: {str(e)}")
                        screenshot_path = f"debug_page_{current_page_num}.png"
                        await page.screenshot(path=screenshot_path)
                        self.logger.info(f"Screenshot saved to {screenshot_path}")
                        break
                    
                    items = []
                    retries = 3
                    while retries > 0:
                        try:
                            items = await self.extract_history_items(page)
                            if items:
                                break
                            else:
                                self.logger.warning(f"No items extracted (retries left: {retries})")
                                retries -= 1
                                await asyncio.sleep(3)
                        except Exception as e:
                            self.logger.warning(f"Error extracting items (retries left: {retries}): {str(e)}")
                            retries -= 1
                            await asyncio.sleep(3)
                    
                    if not items:
                        self.logger.info(f"No items found on page {current_page_num}, stopping")
                        break
                    
                    all_history_items.extend(items)
                    self.logger.info(f"Found {len(items)} items on page {current_page_num}")
                    
                    # Save progress
                    self.process_history_items(items)
                    self.save_item_database()
                    
                    # Pause/Stop controls
                    while self._pause:
                        await asyncio.sleep(1)
                    if self._stop:
                        self.logger.info("Stopping scraping as requested")
                        break
                    
                    # Navigate using buttons
                    nav_container = await page.query_selector("xpath=/html/body/div/main/div[2]/div/div[5]")
                    if not nav_container:
                        self.logger.info("No navigation container found; ending scraping")
                        break
                        
                    page_info_elem = await nav_container.query_selector("xpath=./div/p[1]")
                    if page_info_elem:
                        page_text = (await page_info_elem.text_content()).strip()
                        self.logger.info(f"Navigation info: {page_text}")
                    
                    next_button = await nav_container.query_selector("xpath=.//button[contains(., 'Next')]")
                    if next_button:
                        is_disabled = await next_button.get_attribute("disabled")
                        if is_disabled is not None:
                            self.logger.info("Next button is disabled; ending scraping")
                            break
                        await next_button.click()
                        current_page_num += 1
                        await asyncio.sleep(2)
                    else:
                        self.logger.info("Next button not found; ending scraping")
                        break
                
                self.logger.info(f"Processed {len(all_history_items)} history items")
                return all_history_items
                
            except Exception as e:
                self.logger.error(f"Error during history scraping: {str(e)}", exc_info=True)
                return None
            finally:
                await context.close()
                await browser.close()

    async def extract_history_items(self, page):
        items = []
        await page.wait_for_selector("xpath=/html/body/div/main/div[2]/div/div[4]/table", timeout=15000)
        rows = await page.query_selector_all("xpath=/html/body/div/main/div[2]/div/div[4]/table/tbody/tr")
        if not rows:
            self.logger.debug("No rows found in table")
            return items
        
        self.logger.debug(f"Found {len(rows)} rows to process")
        for i, row in enumerate(rows):
            try:
                await row.scroll_into_view_if_needed()
                
                name_element = await row.query_selector("xpath=./td[1]//span[not(ancestor::small)]")
                name_text = await name_element.text_content() if name_element else None
                if not name_text:
                    self.logger.debug(f"Name not found in row {i}")
                    continue
                
                price_element = await row.query_selector("xpath=./td[2]//div[contains(@class, 'justify-end')]")
                raw_price = await price_element.text_content() if price_element else None
                price_str = re.sub(r"[^\d]", "", raw_price) if raw_price else ""
                if not price_str:
                    self.logger.debug(f"Price not found in row {i}")
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
                self.logger.debug(f"Processed row {i+1}: {item['name']} with price {item['price']}")
            except Exception as e:
                self.logger.warning(f"Error processing row {i}: {str(e)}")
                continue
        
        return items

    def process_history_items(self, items):
        for item in items:
            existing = self.item_db.get(item["name"], [])
            if any(entry["timestamp"] == item["datetime"] and entry["price"] == item["price"] for entry in existing):
                continue
            self.item_db[item["name"]].append({
                "price": item["price"],
                "timestamp": item["datetime"],
                "type": "sale"
            })

    def save_history_snapshot(self, items):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.data_dir, f"history_snapshot_{timestamp}.csv")
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'price', 'datetime']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(items)
            self.logger.info(f"History snapshot saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving history snapshot: {str(e)}")

    def get_item_stats(self, item_name):
        if item_name not in self.item_db or not self.item_db[item_name]:
            return None
        prices = [entry['price'] for entry in self.item_db[item_name]]
        stats = {
            'average_price': sum(prices) / len(prices),
            'median_price': statistics.median(prices),
            'min_price': min(prices),
            'max_price': max(prices),
            'last_sold': self.item_db[item_name][-1] if self.item_db[item_name] else None
        }
        return stats

async def main():
    scraper = SKMarketScraper()
    scraper.logger.info("Starting history scraping process")
    
    history_items = await scraper.scrape_history_pages(max_pages=10)
    
    if history_items:
        scraper.save_history_snapshot(history_items)
        if history_items:
            first_item = history_items[0]['name']
            stats = scraper.get_item_stats(first_item)
            if stats:
                scraper.logger.info(f"Stats for {first_item}:")
                scraper.logger.info(f"  Average Price: {stats['average_price']:.2f}")
                scraper.logger.info(f"  Median Price: {stats['median_price']:.2f}")
                scraper.logger.info(f"  Price Range: {stats['min_price']} - {stats['max_price']}")
                scraper.logger.info(f"  Last Sold: {stats['last_sold']}")
    
    scraper.logger.info("History scraping process completed")

if __name__ == "__main__":
    asyncio.run(main())
