import asyncio
import logging
import os
from apify_client import ApifyClient
from apify_client.consts import ActorEventTypes

from scraper_utils import _expand_sitemap_to_page_urls_async, scrape_page_and_extract_product, _product_url_heuristic
from browser_like_http import AsyncScraperHTTP

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting Apify Actor...")

    apify_client = ApifyClient(os.environ["APIFY_TOKEN"])
    actor_input = await apify_client.get_value("INPUT")

    if not actor_input:
        logger.error("Actor input is missing or empty.")
        return

    start_urls_input = actor_input.get("startUrls", [])
    if not start_urls_input:
        logger.warning("No start URLs provided in the input.")
        return

    # Use a RequestQueue to manage URLs to scrape
    request_queue = await apify_client.request_queues().get_or_create()

    # Add initial URLs to the request queue
    for item in start_urls_input:
        if "url" in item:
            await request_queue.add_request({'url': item['url']})

    async with AsyncScraperHTTP() as fetcher:
        while True:
            request = await request_queue.fetch_next_request()
            if not request:
                break

            url = request['url']
            logger.info(f"Processing URL: {url}")

            try:
                # Check if it's a sitemap URL first
                if url.endswith('/sitemap.xml') or 'sitemap' in url:
                    logger.info(f"Expanding sitemap: {url}")
                    expanded_urls = await _expand_sitemap_to_page_urls_async(fetcher, url)
                    product_urls_from_sitemap = [u for u in expanded_urls if _product_url_heuristic(u)]
                    logger.info(f"Found {len(product_urls_from_sitemap)} product URLs from sitemap {url}")
                    for product_url in product_urls_from_sitemap:
                        await request_queue.add_request({'url': product_url})
                    await request_queue.mark_request_as_handled(request)
                else:
                    # Assume it's a product page or a page that might contain JSON-LD
                    product_data = await scrape_page_and_extract_product(fetcher, url)
                    if product_data:
                        await apify_client.datasets().get_or_create().push_items([product_data])
                        logger.info(f"Successfully scraped and pushed data for {url}")
                        await request_queue.mark_request_as_handled(request)
                    else:
                        logger.warning(f"No product data extracted from {url}. Reclaiming request.")
                        await request_queue.reclaim_request(request)

            except Exception as e:
                logger.error(f"Failed to process {url}: {e}", exc_info=True)
                await request_queue.reclaim_request(request)

    logger.info("Apify Actor finished.")

if __name__ == "__main__":
    asyncio.run(main())
