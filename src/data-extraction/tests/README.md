# API Response Validation Tests

This test suite validates the Aldi webshop API responses to detect any breaking changes that might require code updates.

## Purpose

The webshop API can change without notice, breaking our scraper. These tests help:
- Detect API structure changes early
- Validate critical fields our application depends on
- Ensure price formats remain consistent
- Verify pagination metadata

## Test Structure

### Unit Tests (`tests/unit/`)
- Test validation logic with mock data
- Fast, no network calls
- Run frequently during development

### Integration Tests (`tests/integration/`)
- Test against live Aldi API
- Detect real-world API changes
- Should be run before deployments

## Running Tests

```bash
# Run all tests
yarn test

# Run only unit tests (fast, offline)
yarn test:unit

# Run integration tests (requires internet)
yarn test:integration

# Run tests in watch mode during development
yarn test:watch

# Generate coverage report
yarn test:coverage

# Validate API contract (standalone validator)
yarn validate:api
```

## What Gets Validated

### 1. API Response Structure
- Meta object with pagination info
- Data array containing products
- All required fields present

### 2. Product Structure
- All fields match expected types
- Nested objects (price, categories, assets)
- Field formats (SKU, URL slugs)

### 3. Critical Fields (Used by our app)
- `sku`: Product identifier
- `name`: Product name
- `brandName`: Brand name
- `sellingSize`: Package size
- `price.amountRelevantDisplay`: Formatted price (must be £X.XX format)

### 4. Price Format Validation
- Currency must be GBP
- Symbol must be £
- Display formats must match pattern
- Comparison prices properly formatted

## When Tests Fail

If integration tests fail, it likely means:

1. **API Structure Changed**: Check error messages for missing/changed fields
2. **Price Format Changed**: Currency or display format may have changed
3. **New Required Fields**: API may require new fields we don't handle

### Next Steps When Tests Fail:

1. Run `yarn validate:api` for detailed validation report
2. Check the sample product structure in the output
3. Update schemas in `src/schemas/aldi-response.schema.ts`
4. Update types in `src/types/index.ts`
5. Update scraper logic if needed
6. Re-run tests to confirm fixes

## Continuous Monitoring

Consider running integration tests:
- Daily in CI/CD pipeline
- Before each deployment
- When scraper errors increase

This proactive approach helps catch API changes before they break production.