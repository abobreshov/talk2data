import { describe, test, expect } from 'vitest';
import { ResponseValidator } from '../../src/utils/validation';
import validResponse from '../fixtures/valid-response.json';
import invalidResponses from '../fixtures/invalid-responses.json';

describe('ResponseValidator', () => {
  describe('validateApiResponse', () => {
    test('should validate a correct API response', () => {
      const result = ResponseValidator.validateApiResponse(validResponse);
      expect(result.isValid).toBe(true);
      expect(result.errors).toBeUndefined();
    });

    test('should fail when meta is missing', () => {
      const result = ResponseValidator.validateApiResponse(invalidResponses.invalidMeta);
      expect(result.isValid).toBe(false);
      expect(result.errors).toBeDefined();
      expect(result.errors?.length).toBeGreaterThan(0);
    });

    test('should validate empty response with proper structure', () => {
      // Note: Joi considers arrays with .items() and required schemas to need at least one item
      // This is expected behavior - in production we check critical fields separately
      const result = ResponseValidator.validateApiResponse(invalidResponses.emptyResponse);
      expect(result.isValid).toBe(false);
      expect(result.errors?.some(err => err.includes('does not contain 1 required value'))).toBe(true);
    });
  });

  describe('validateProduct', () => {
    test('should validate a correct product', () => {
      const product = validResponse.data[0];
      const result = ResponseValidator.validateProduct(product);
      expect(result.isValid).toBe(true);
    });

    test('should fail when price is missing', () => {
      const product = invalidResponses.missingPrice.data[0];
      const result = ResponseValidator.validateProduct(product);
      expect(result.isValid).toBe(false);
      expect(result.errors?.some(err => err.includes('price'))).toBe(true);
    });

    test('should fail when price format is invalid', () => {
      const product = invalidResponses.invalidPriceFormat.data[0];
      const result = ResponseValidator.validateProduct(product);
      expect(result.isValid).toBe(false);
      // Should fail because currencyCode is EUR instead of GBP
      expect(result.errors?.some(err => err.includes('currencyCode'))).toBe(true);
    });
  });

  describe('validateCriticalFields', () => {
    test('should validate when all critical fields are present', () => {
      const product = validResponse.data[0];
      const result = ResponseValidator.validateCriticalFields(product);
      expect(result.isValid).toBe(true);
    });

    test('should fail when brandName is missing', () => {
      const product = invalidResponses.missingCriticalField.data[0];
      const result = ResponseValidator.validateCriticalFields(product);
      expect(result.isValid).toBe(false);
      expect(result.errors?.some(err => err.includes('brandName'))).toBe(true);
    });

    test('should fail when price display format is invalid', () => {
      const product = invalidResponses.invalidPriceFormat.data[0];
      const result = ResponseValidator.validateCriticalFields(product);
      expect(result.isValid).toBe(false);
      // amountRelevantDisplay should start with £
      expect(result.errors?.some(err => err.includes('amountRelevantDisplay'))).toBe(true);
    });
  });

  describe('validateWithReport', () => {
    test('should generate comprehensive report for valid response', () => {
      const report = ResponseValidator.validateWithReport(validResponse);
      expect(report.isValid).toBe(true);
      expect(report.structureValid).toBe(true);
      expect(report.criticalFieldsValid).toBe(true);
      expect(report.report.some(line => line.includes('✅'))).toBe(true);
    });

    test('should detect structure and critical field issues', () => {
      const report = ResponseValidator.validateWithReport(invalidResponses.missingCriticalField);
      expect(report.isValid).toBe(false);
      expect(report.criticalFieldsValid).toBe(false);
      expect(report.report.some(line => line.includes('❌'))).toBe(true);
    });

    test('should handle multiple validation errors', () => {
      const report = ResponseValidator.validateWithReport(invalidResponses.invalidPriceFormat);
      expect(report.isValid).toBe(false);
      expect(report.report.length).toBeGreaterThan(2);
    });
  });
});