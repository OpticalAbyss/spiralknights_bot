# Spiral Knights Auction Tools

This repository contains two main tools built with Python and Playwright to help analyze auctions on the Spiral Knights Auction House using ApeTrader's Auction House listing Website:

1. **History Scraper:**  
   Scrapes historical auction data from the auction house and stores it in JSON and CSV formats. This data is used later to evaluate current auctions.

2. **Current Listing Scraper & Evaluator:**  
   Navigates through the current auction listings (using Next.js–style pagination) by clicking the Next button, extracts current auction data, and compares current prices with historical auction data to generate recommendations (bid, buyout, or skip).

---

## Prerequisites

- **Python 3.8+**  
- **Playwright for Python**  
  Install via pip and then install browser binaries:
  ```bash
  pip install playwright
  playwright install
  ```
- Standard Python libraries used include: `asyncio`, `re`, `json`, `os`, `datetime`, `collections`, `statistics`, `logging`, and `csv`.

---

## 1. History Scraper

### Overview

The History Scraper navigates through auction history pages on the auction website and extracts data such as:

- Item Name
- Price
- Auction Date & Time

It processes pages in batches (with configurable batch size and concurrency) to reduce risk of data loss from any failure. The scraped data is stored in `sk_market_data/item_database.json` and snapshots of each batch are saved as CSV files for backup and debugging.

---

## 2. Current Listing Scraper & Evaluator

### Overview

The Current Listing Scraper navigates through the current auction listings on the website—where pagination is controlled by buttons rather than URL parameters—and extracts details such as:

- Item Name
- Bid Price
- Buyout Price
- Time Left

The Evaluator then compares the current auction prices against historical prices (using the data collected by the History Scraper) to generate recommendations (bid, buyout, or skip) based on whether the current price is lower than the historical median.

---

## Configuration & Customization

- **XPaths:**  
  Both tools rely on specific XPaths to identify elements on the page (for history tables, current listings, navigation buttons, etc.). Adjust these XPaths if the website’s structure changes.

- **Batch Size & Concurrency:**  
  The History Scraper uses batch processing and persistent workers to avoid restarting from page 1 each time. You can modify the batch size and the number of concurrent workers to suit your system and reduce navigation overhead.

- **Evaluation Criteria:**  
  The evaluation in the Current Listing Scraper is based on a simple comparison between the current price and the historical median. You can enhance this logic to consider additional factors.

---

## Running the Tools

To run each tool, simply execute the corresponding script via Python:

- History Scraper:
     - Make sure to adjust the number of worker, the total number of pages and the batch size before running.
  ```bash
  python headless_main.py
  ```
- Current Listing Scraper & Evaluator:
  ```bash
  python bid_analyzer.py
  ```
  
---

## Limitation

- Currenty the script can't extract UV from weapon listings. So i.e it doesn't distingush between a CTR VH autogun and a clean one, This skews the results for most weapons.
- It uses ApeTrader's Website which is a NextJS app, therefore scrapping it requires alot of resources to load in the pages.
- The script is has heavy requirements, but not as massive as downloading the game and getting a mod to check those prices(if it even exists).
- The data from the website is not complete. The historical listings only shows the buyout price and not what the listing actually sold for, this is in the case that the item had buyout. In the case of the listing only having a bid price it works fine.
- The script will take an hour or two to scrap the entire history which is currently 5910 pages. 

---

## Limitation

- Making more informed desicions on purchases from the market.
- You Could use it as a merching assistant tool.
- Analysis of data

### Some quick analysis I did on one of the snapshots to see the trend of QQQ Slime Lockboxes. Only for 12 days.
![image](https://github.com/user-attachments/assets/3ef1263a-bfbd-4b7c-90ca-c05c748d670d)


## Contact

For questions or issues, please create an issue in this repository.
Give this repository a star while you're at it. I'd appreciate it.
