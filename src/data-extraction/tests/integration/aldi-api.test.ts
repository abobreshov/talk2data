import { describe, test, expect, beforeAll } from 'vitest';
import * as dotenv from 'dotenv';
import { fetchWithRateLimit } from '../../src/utils/http';
import { ResponseValidator } from '../../src/utils/validation';
import { AldiConfig } from '../../src/types';

dotenv.config();

describe('Aldi API Contract Tests', () => {
  let config: AldiConfig;
  let testUrl: string;

  beforeAll(() => {
    config = {
      baseUrl: process.env.ALDI_BASE_URL || '',
      currency: process.env.ALDI_CURRENCY || '',
      serviceType: process.env.ALDI_SERVICE_TYPE || '',
      sort: process.env.ALDI_SORT || '',
      testVariant: process.env.ALDI_TEST_VARIANT || '',
      servicePoint: process.env.ALDI_SERVICE_POINT || '',
      getNotForSaleProducts: parseInt(process.env.ALDI_GET_NOT_FOR_SALE_PRODUCTS || '0')
    };

    // Test with a small limit to make the test faster
    const params = new URLSearchParams({
      currency: config.currency,
      serviceType: config.serviceType,
      categoryKey: '1588161416978051001', // Milk category
      limit: '12', // API requires specific values: [12,16,24,30,32,48,60]
      offset: '0',
      getNotForSaleProducts: config.getNotForSaleProducts.toString(),
      sort: config.sort,
      testVariant: config.testVariant,
      servicePoint: config.servicePoint
    });

    testUrl = `${config.baseUrl}?${params.toString()}`;
  });

  test('should have valid configuration', () => {
    expect(config.baseUrl).toBeTruthy();
    expect(config.currency).toBe('GBP');
    expect(config.serviceType).toBe('walk-in');
  });

  test('API response should match expected schema', async () => {
    const response = await fetchWithRateLimit(testUrl, false); // No progress display in tests
    
    // Validate complete response structure
    const structureValidation = ResponseValidator.validateApiResponse(response);
    expect(structureValidation.isValid).toBe(true);
    
    if (!structureValidation.isValid) {
      console.error('Structure validation errors:', structureValidation.errors);
    }
  }, 30000); // 30 second timeout for API call

  test('API products should contain all critical fields', async () => {
    const response = await fetchWithRateLimit(testUrl, false);
    
    expect(response.data).toBeDefined();
    expect(Array.isArray(response.data)).toBe(true);
    
    // Test each product for critical fields
    response.data.forEach((product, index) => {
      const criticalValidation = ResponseValidator.validateCriticalFields(product);
      expect(criticalValidation.isValid).toBe(true);
      
      if (!criticalValidation.isValid) {
        console.error(`Product ${index} critical field errors:`, criticalValidation.errors);
      }
      
      // Additional assertions for our specific needs
      expect(product.sku).toBeTruthy();
      expect(product.name).toBeTruthy();
      expect(product.brandName).toBeTruthy();
      expect(product.sellingSize).toBeTruthy();
      expect(product.price.amountRelevantDisplay).toMatch(/^£\d+\.\d{2}$/);
    });
  }, 30000);

  test('API response should have valid pagination metadata', async () => {
    const response = await fetchWithRateLimit(testUrl, false);
    
    expect(response.meta).toBeDefined();
    expect(response.meta.pagination).toBeDefined();
    expect(response.meta.pagination.totalCount).toBeGreaterThanOrEqual(0);
    expect(typeof response.meta.pagination.totalCount).toBe('number');
    expect(response.meta.pagination.offset).toBeDefined();
    expect(response.meta.pagination.limit).toBeDefined();
  }, 30000);

  test('Price structure should match expected format', async () => {
    const response = await fetchWithRateLimit(testUrl, false);
    
    if (response.data.length > 0) {
      const firstProduct = response.data[0];
      const price = firstProduct.price;
      
      // Verify price structure
      expect(price).toBeDefined();
      expect(price.amount).toBeGreaterThanOrEqual(0);
      expect(price.currencyCode).toBe('GBP');
      expect(price.currencySymbol).toBe('£');
      expect(price.amountRelevantDisplay).toMatch(/^£\d+\.\d{2}$/);
      expect(price.bottleDepositDisplay).toMatch(/^£\d+\.\d{2}$/);
      
      // Check comparison display format if present
      if (price.comparisonDisplay) {
        expect(price.comparisonDisplay).toMatch(/^£\d+\.\d{2}\//);
      }
    }
  }, 30000);

  test('should generate validation report', async () => {
    const response = await fetchWithRateLimit(testUrl, false);
    const validation = ResponseValidator.validateWithReport(response);
    
    console.log('\n=== API Contract Validation Report ===');
    validation.report.forEach(line => console.log(line));
    console.log('=====================================\n');
    
    expect(validation.isValid).toBe(true);
    expect(validation.structureValid).toBe(true);
    expect(validation.criticalFieldsValid).toBe(true);
  }, 30000);
});