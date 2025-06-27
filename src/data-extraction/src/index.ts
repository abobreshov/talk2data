import { AldiScraper } from './scrapers/aldi';
import { displayError } from './utils/display';

async function main() {
  try {
    const scraper = new AldiScraper();
    await scraper.scrape();
  } catch (error) {
    displayError(error as Error);
    process.exit(1);
  }
}

main().catch(error => {
  displayError(error as Error);
  process.exit(1);
});