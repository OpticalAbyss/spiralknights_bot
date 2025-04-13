import asyncio
import re
import json
import os
from datetime import datetime
from collections import defaultdict
import statistics
import logging
from playwright.async_api import async_playwright
import csv

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AuctionEvaluator")

class AuctionEvaluator:
    def __init__(self, history_db_path="sk_market_data/item_database.json"):
        self.history_db_path = history_db_path
        self.history_data = self.load_history_data()

    def load_history_data(self):
        """Load historical auction data from the JSON file."""
        if os.path.exists(self.history_db_path):
            with open(self.history_db_path, "r") as f:
                return json.load(f)
        else:
            logger.info("History database not found.")
            return {}

    def get_historical_price_stats(self, item_name):
        """Return basic stats for an item, if available."""
        data = self.history_data.get(item_name)
        if data:
            prices = [sale.get("price") for sale in data if isinstance(sale.get("price"), (int, float))]
            if prices:
                return {
                    "min": min(prices),
                    "average": sum(prices) / len(prices),
                    "median": statistics.median(prices)
                }
        return None

    def parse_time_left(self, time_text: str) -> int:
        """
        Parse the "Time Left" text into minutes.
        For "-" or "Very Short", returns 0.
        Supports strings like "5m", "30m", "1h" or "1h30m".
        """
        raw = time_text.strip().lower()
        logger.debug(f"Raw time left text: '{raw}'")
        if raw in ("-", "very short"):
            return 0
        minutes = 0
        hour_match = re.search(r'(\d+)\s*h', raw)
        minute_match = re.search(r'(\d+)\s*m', raw)
        if hour_match:
            minutes += int(hour_match.group(1)) * 60
        if minute_match:
            minutes += int(minute_match.group(1))
        if not hour_match and not minute_match:
            try:
                minutes = int(raw)
            except ValueError:
                logger.warning(f"Could not parse time left: '{time_text}'")
                minutes = 0
        logger.debug(f"Parsed time left: '{time_text}' -> {minutes} minutes")
        return minutes

    async def extract_listings_from_page(self, page) -> list:
        """
        Extract auction listings from the current page.
        Each TR represents one auction listing.
        """
        listings = []
        rows_selector = "xpath=/html/body/div/main/div[2]/div/div[3]/table/tbody/tr"
        try:
            await page.wait_for_selector(rows_selector, timeout=30000, state="attached")
        except Exception as e:
            logger.error(f"Auction rows not found after 30s: {e}")
            return listings
        rows = await page.query_selector_all(rows_selector)
        logger.info(f"Found {len(rows)} table rows on the auctions page.")
        for row in rows:
            try:
                await row.scroll_into_view_if_needed()
                # Item name in td[1]
                name_td = await row.query_selector("xpath=./td[1]")
                item_name = (await name_td.text_content()).strip() if name_td else "Unknown"
                # Bid price in td[2]
                bid_td = await row.query_selector("xpath=./td[2]")
                bid_text = await bid_td.text_content() if bid_td else ""
                bid_text = re.sub(r"[^\d]", "", bid_text)
                if not bid_text:
                    continue
                bid_price = int(bid_text)
                # Buyout price in td[3]
                buyout_td = await row.query_selector("xpath=./td[3]")
                buyout_text = await buyout_td.text_content() if buyout_td else ""
                buyout_text = re.sub(r"[^\d]", "", buyout_text) if buyout_text else ""
                buyout_price = int(buyout_text) if buyout_text else None
                # Time left in td[4]
                time_td = await row.query_selector("xpath=./td[4]")
                time_text = await time_td.text_content() if time_td else ""
                time_left = self.parse_time_left(time_text)
                listings.append({
                    "name": item_name,
                    "bid_price": bid_price,
                    "buyout_price": buyout_price,
                    "time_left": time_left,
                    "raw_time": time_text.strip()
                })
            except Exception as ex:
                logger.error(f"Error processing a row: {ex}")
                continue
        return listings

    async def evaluate_all_auctions(self):
        """
        Navigate through the auctions pagination by clicking the Next button,
        and extract all auction listings across all pages.
        """
        all_listings = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1280, "height": 1024})
            await context.route("**/*", lambda route, req: route.abort() if req.resource_type in ["image", "stylesheet", "font"] else route.continue_())
            page = await context.new_page()
            
            auctions_url = "https://www.sk-ah.com/"  # Adjust if needed.
            await page.goto(auctions_url, timeout=60000)
            await page.wait_for_load_state("networkidle")
            
            # Get total pages from the page info element.
            page_info_selector = "xpath=/html/body/div/main/div[2]/div/div[4]/div/p[1]"
            try:
                await page.wait_for_selector(page_info_selector, timeout=30000, state="attached")
            except Exception as e:
                logger.error(f"Page info element not found: {e}")
                return all_listings
            
            page_info_elem = await page.query_selector(page_info_selector)
            page_info_text = (await page_info_elem.text_content()).strip() if page_info_elem else ""
            m = re.search(r'Page \d+\s+of\s+(\d+)', page_info_text)
            total_pages = int(m.group(1)) if m else 1
            logger.info(f"Total auction pages: {total_pages}")
            
            current_page = 1
            while current_page <= total_pages:
                logger.info(f"Processing page {current_page} of {total_pages}")
                listings = await self.extract_listings_from_page(page)
                all_listings.extend(listings)
                logger.info(f"Extracted {len(listings)} listings from page {current_page}")
                
                if current_page >= total_pages:
                    break
                
                # Wait for the Next button to be visible.
                next_button_selector = "xpath=/html/body/div/main/div[2]/div/div[4]/button[2]"
                try:
                    await page.wait_for_selector(next_button_selector, timeout=10000, state="visible")
                except Exception as e:
                    logger.error(f"Next button not found on page {current_page}: {e}")
                    break
                
                next_button = await page.query_selector(next_button_selector)
                if next_button:
                    is_disabled = await next_button.get_attribute("disabled")
                    if is_disabled is not None:
                        logger.info("Next button is disabled; reached last page.")
                        break
                    await next_button.click()
                    # Wait for the page info element to update
                    for _ in range(10):
                        await asyncio.sleep(1)
                        new_page_info_elem = await page.query_selector(page_info_selector)
                        new_page_text = (await new_page_info_elem.text_content()).strip() if new_page_info_elem else ""
                        m = re.search(r'Page (\d+)', new_page_text)
                        if m:
                            new_page = int(m.group(1))
                            if new_page > current_page:
                                current_page = new_page
                                break
                else:
                    logger.info("Next button not found; ending pagination.")
                    break

            await context.close()
            await browser.close()
        
        logger.info(f"Total listings extracted from all pages: {len(all_listings)}")
        return all_listings

    async def evaluate_auctions(self):
        """
        Combines extracting auctions from all pages with evaluation using historical data,
        then saves outputs.
        """
        all_listings = await self.evaluate_all_auctions()
        if not all_listings:
            logger.info("No listings extracted.")
            return

        # Save full listings to CSV for debugging.
        full_output_file = "auction_full_listings.csv"
        try:
            with open(full_output_file, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["name", "bid_price", "buyout_price", "time_left", "raw_time"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_listings)
            logger.info(f"All auction listings saved to {full_output_file}")
        except Exception as e:
            logger.error(f"Error saving full listings: {e}")

        # Evaluate each listing against historical data.
        recommendations = []
        for listing in all_listings:
            stats = self.get_historical_price_stats(listing["name"])
            rec = {
                "name": listing["name"],
                "bid_price": listing["bid_price"],
                "buyout_price": listing["buyout_price"],
                "time_left": listing["time_left"],
                "raw_time": listing["raw_time"]
            }
            if stats:
                rec["historical_median"] = stats["median"]
                if listing["bid_price"] < stats["median"]:
                    rec["action"] = "bid"
                elif listing["buyout_price"] is not None and listing["buyout_price"] < stats["median"]:
                    rec["action"] = "buyout"
                else:
                    rec["action"] = "skip"
            else:
                rec["action"] = "no history"
            recommendations.append(rec)

        rec_file = "auction_recommendations.csv"
        try:
            with open(rec_file, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["name", "bid_price", "buyout_price", "time_left", "raw_time", "historical_median", "action"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(recommendations)
            logger.info(f"Recommendations saved to {rec_file}")
        except Exception as e:
            logger.error(f"Error saving recommendations: {e}")

        logger.info("Evaluation complete. Recommendations:")
        for rec in recommendations:
            logger.info(f"{rec['name']}: Bid {rec['bid_price']}, Buyout {rec['buyout_price']}, "
                        f"Time Left {rec['time_left']}m (raw: '{rec['raw_time']}'), "
                        f"Historical Median: {rec.get('historical_median', 'N/A')} -> {rec['action']}")
        return recommendations

async def main():
    evaluator = AuctionEvaluator()
    await evaluator.evaluate_auctions()

if __name__ == "__main__":
    asyncio.run(main())
