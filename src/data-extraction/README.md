## Project Overview

This is a data extraction service for scraping grocery product data from UK grocery stores (currently Aldi). Part of the "talk2data" grocery POC system. Built with TypeScript, it uses modern web scraping techniques with anti-bot protection. The project is fully implemented with working scrapers, validation, and comprehensive testing.

## Technology Stack

- **Language**: TypeScript with strict mode
- **Package Manager**: Yarn 4.9.2
- **Module System**: NodeNext (modern ES modules with Node.js extensions)
- **Target**: ES2016
- **Testing**: Vitest framework
- **Key Dependencies**:
  - `got-scraping`: Web scraping with anti-bot protection
  - `fast-csv`: CSV data processing
  - `joi`: Schema validation
  - `chalk`: Terminal output formatting
  - `cli-progress`: Progress tracking
  - `ora`: Elegant terminal spinners
  - `boxen`: Terminal boxes for messages
  - `dotenv`: Environment configuration

## Development Commands

```bash
# Install dependencies
yarn install

# Run the scraper
yarn start           # Run main scraper
yarn scrape          # Alternative command
yarn scrape:aldi     # Aldi-specific scraper

# Build TypeScript
yarn build          # Compile to JavaScript

# Testing
yarn test            # Run all tests
yarn test:unit       # Unit tests only (offline)
yarn test:integration # Integration tests (requires internet)
yarn test:coverage   # Generate coverage report

# Validation
yarn validate:api    # Validate API response structure
```

## Architecture

The project implements a complete data extraction pipeline:

```
data-extraction/
├── aldi/                    # Aldi-specific implementation
│   └── config/             
│       └── product-categories.csv  # Category mapping (id, name, URL slug)
├── src/
│   ├── scrapers/           # Store-specific scrapers
│   │   └── aldi/          
│   │       ├── index.ts    # Main scraper implementation
│   │       └── config.ts   # Configuration
│   ├── schemas/            # Joi validation schemas
│   │   └── aldi/          
│   │       └── product-schema.ts
│   └── utils/              # Utility functions
│       ├── csv-reader.ts   # CSV processing
│       ├── logger.ts       # Logging utilities
│       └── validation.ts   # Data validation
├── tests/                  # Comprehensive test suite
│   ├── unit/              # Offline tests
│   ├── integration/       # Live API tests
│   └── fixtures/          # Test data
├── tools/                  # Utility scripts
│   └── validate-api.ts    # API validation tool
├── data/                   # Output directory (generated)
└── coverage/              # Test coverage reports (generated)
```

## Key Implementation Details

1. **Store-specific structure**: Each grocery store (e.g., Aldi) has its own directory with configuration and implementation
2. **Category mapping**: The CSV files map category IDs to names and URL slugs for API calls
3. **Validation-first approach**: All API responses are validated using Joi schemas before processing
4. **Rate limiting**: Implements random delays (2-10 seconds) between API requests to avoid detection
5. **Progressive data fetching**: Processes 30 products per page, handles pagination
6. **Error handling**: Comprehensive error handling with retries and logging
7. **Testing strategy**: Unit tests for validation logic, integration tests for API contract

## Environment Configuration

Create `.env` file from `.env.example`:
```env
ALDI_BASE_URL=https://api.aldi.co.uk/v3/product-search
ALDI_CURRENCY=GBP
ALDI_SERVICE_TYPE=walk-in
ALDI_SORT=relevance
ALDI_TEST_VARIANT=A
ALDI_SERVICE_POINT=C605
ALDI_GET_NOT_FOR_SALE_PRODUCTS=1
```

## Data Flow

1. **Load categories**: Read from `product-categories.csv`
2. **Fetch products**: Call Aldi API for each category with pagination
3. **Validate responses**: Use Joi schemas to ensure data integrity
4. **Transform data**: Extract required fields including category hierarchy
5. **Save to CSV**: Output to `data/products.csv` with format:
   ```
   sku, name, brandName, sellingSize, currency, price, category, subcategory
   ```
   - Category data is extracted from the API response's categories array
   - First category is the main category, second is the subcategory (if present)

## Testing

- **Unit tests**: Test validation schemas with mock data (no network calls)
- **Integration tests**: Test against live Aldi API to detect breaking changes
- **API validator**: Standalone tool for contract compliance checking
- **Coverage reports**: Full test coverage analysis available

Test fixtures:
- `tests/fixtures/valid-response.json` - Expected API response format
- `tests/fixtures/invalid-responses.json` - Examples of malformed responses

## Build Artifacts

The project compiles TypeScript to JavaScript:
- Source files: `src/**/*.ts`
- Compiled files: `src/**/*.js` (generated)
- Both exist side-by-side after build
