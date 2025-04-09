import asyncio
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

    async def scrape_history_pages(self, max_pages=10):
        """Scrape all available history pages"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                self.logger.info(f"Starting history scraping (max {max_pages} pages)")
                current_page = 1
                all_history_items = []
                
                while current_page <= max_pages:
                    url = f"{self.history_url}?page={current_page}" if current_page > 1 else self.history_url
                    self.logger.info(f"Scraping history page {current_page} - {url}")
                    
                    await page.goto(url, timeout=60000)
                    
                    # Wait for history table to load
                    try:
                        await page.wait_for_selector('tr.border-b', timeout=15000)
                        self.logger.debug("History table rows found")
                    except Exception as e:
                        self.logger.warning(f"History table not found on page {current_page}: {str(e)}")
                        break
                    
                    # Extract history items
                    items = await self.extract_history_items(page)
                    if not items:
                        self.logger.info(f"No items found on page {current_page}, stopping")
                        break
                    
                    all_history_items.extend(items)
                    self.logger.info(f"Found {len(items)} items on page {current_page}")
                    
                    # Check if there's a next page
                    next_page_btn = await page.query_selector('a[aria-label="Next page"]:not([disabled])')
                    if not next_page_btn:
                        self.logger.info("No more pages available")
                        break
                    
                    current_page += 1
                    await asyncio.sleep(2)  # Be polite with delay between pages
                
                # Process all collected history items
                if all_history_items:
                    self.process_history_items(all_history_items)
                    self.save_item_database()
                    self.logger.info(f"Processed {len(all_history_items)} history items from {current_page-1} pages")
                else:
                    self.logger.warning("No history items found")
                
                return all_history_items
                
            except Exception as e:
                self.logger.error(f"Error during history scraping: {str(e)}", exc_info=True)
                return None
            finally:
                await context.close()
                await browser.close()

    async def extract_history_items(self, page):
        """Extract items from the current history page"""
        items = []
        rows = await page.query_selector_all('tr.border-b')
        
        for row in rows:
            try:
                # Extract item name and quantity
                name_element = await row.query_selector('div.flex.flex-col > span')
                name_text = await name_element.text_content()
                name_parts = name_text.split(' x')
                name = name_parts[0].strip()
                quantity = int(name_parts[1].strip()) if len(name_parts) > 1 else 1
                
                # Extract price (crowns)
                price_element = await row.query_selector('div.flex.items-center.justify-end.gap-1')
                price_text = await price_element.text_content()
                price = int(price_text.strip().replace(',', ''))
                
                # Extract status (Sold)
                status_element = await row.query_selector('small.text-xs.text-gray-500.dark\\:text-gray-400')
                status_text = await status_element.text_content()
                status = status_text.strip()
                
                # Extract date and time
                date_element = await row.query_selector('div.flex.items-center.justify-end.gap-1')
                date_text = await date_element.text_content()
                time_element = await row.query_selector('small.text-xs.text-gray-500.dark\\:text-gray-400')
                time_text = await time_element.text_content()
                datetime_str = f"{date_text.strip()} {time_text.strip()}"
                
                # Create item dictionary
                item = {
                    'name': name,
                    'quantity': quantity,
                    'price': price,
                    'status': status,
                    'datetime': datetime_str,
                    'price_per_unit': price / quantity if quantity > 1 else price
                }
                
                items.append(item)
                
            except Exception as e:
                self.logger.warning(f"Error processing history row: {str(e)}")
                continue
        
        return items

    def process_history_items(self, items):
        """Add history items to the database"""
        for item in items:
            # Convert datetime string to datetime object
            try:
                dt = datetime.strptime(item['datetime'], '%m/%d/%Y %I:%M:%S %p')
            except ValueError:
                dt = datetime.now()  # fallback to current time if parsing fails
                
            self.item_db[item['name']].append({
                'price': item['price'],
                'price_per_unit': item['price_per_unit'],
                'quantity': item['quantity'],
                'status': item['status'],
                'timestamp': dt.isoformat(),
                'type': 'sale'
            })

    def get_item_stats(self, item_name):
        """Get statistics for a specific item"""
        if item_name not in self.item_db or not self.item_db[item_name]:
            self.logger.warning(f"No data found for item: {item_name}")
            return None
        
        try:
            price_history = self.item_db[item_name]
            prices = [entry['price_per_unit'] for entry in price_history if 'price_per_unit' in entry]
            
            if not prices:
                return None
                
            stats = {
                'name': item_name,
                'average_price': statistics.mean(prices),
                'median_price': statistics.median(prices),
                'min_price': min(prices),
                'max_price': max(prices),
                'price_count': len(prices),
                'last_sold': price_history[-1] if price_history else None,
                'price_history': price_history[-30:]  # Last 30 entries
            }
            
            self.logger.debug(f"Generated stats for {item_name}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error generating stats for {item_name}: {str(e)}")
            return None

    def save_history_snapshot(self, items):
        """Save current history snapshot to CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.data_dir, f"history_snapshot_{timestamp}.csv")
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'quantity', 'price', 'price_per_unit', 'status', 'datetime']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(items)
            self.logger.info(f"History snapshot saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving history snapshot: {str(e)}")

async def main():
    scraper = SKMarketScraper()
    scraper.logger.info("Starting history scraping process")
    
    # Scrape history pages
    history_items = await scraper.scrape_history_pages()
    
    if history_items:
        # Save the history snapshot
        scraper.save_history_snapshot(history_items)
        
        # Get stats for a sample item
        sample_item = history_items[0]['name'] if history_items else None
        if sample_item:
            stats = scraper.get_item_stats(sample_item)
            
            if stats:
                scraper.logger.info(f"\nStats for {sample_item}:")
                scraper.logger.info(f"Average Price: {stats['average_price']:.2f}")
                scraper.logger.info(f"Median Price: {stats['median_price']:.2f}")
                scraper.logger.info(f"Price Range: {stats['min_price']:.2f} - {stats['max_price']:.2f}")
                scraper.logger.info(f"Last Sold Price: {stats['last_sold']['price_per_unit'] if stats['last_sold'] else 'N/A'}")
                scraper.logger.info(f"Last Sold Quantity: {stats['last_sold']['quantity'] if stats['last_sold'] else 'N/A'}")
                scraper.logger.info(f"Last Sold Date: {stats['last_sold']['timestamp'] if stats['last_sold'] else 'N/A'}")
    
    scraper.logger.info("History scraping process completed")

if __name__ == "__main__":
    asyncio.run(main())