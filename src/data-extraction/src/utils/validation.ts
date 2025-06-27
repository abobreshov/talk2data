import { apiResponseSchema, criticalFieldsSchema, productSchema } from '../schemas/aldi-response.schema';

export interface ValidationResult {
  isValid: boolean;
  errors?: string[];
  warnings?: string[];
}

export class ResponseValidator {
  /**
   * Validates the complete API response structure
   */
  static validateApiResponse(response: any): ValidationResult {
    const result = apiResponseSchema.validate(response, { 
      abortEarly: false,
      allowUnknown: false 
    });

    if (result.error) {
      return {
        isValid: false,
        errors: result.error.details.map(detail => detail.message)
      };
    }

    return { isValid: true };
  }

  /**
   * Validates individual product structure
   */
  static validateProduct(product: any): ValidationResult {
    const result = productSchema.validate(product, { 
      abortEarly: false,
      allowUnknown: false 
    });

    if (result.error) {
      return {
        isValid: false,
        errors: result.error.details.map(detail => detail.message)
      };
    }

    return { isValid: true };
  }

  /**
   * Validates only the critical fields we use in our application
   */
  static validateCriticalFields(product: any): ValidationResult {
    const result = criticalFieldsSchema.validate(product, { 
      abortEarly: false 
    });

    if (result.error) {
      return {
        isValid: false,
        errors: result.error.details.map(detail => detail.message)
      };
    }

    return { isValid: true };
  }

  /**
   * Performs a comprehensive validation check and returns detailed report
   */
  static validateWithReport(response: any): {
    isValid: boolean;
    structureValid: boolean;
    criticalFieldsValid: boolean;
    report: string[];
  } {
    const report: string[] = [];
    let structureValid = true;
    let criticalFieldsValid = true;

    // Validate overall structure
    const structureResult = this.validateApiResponse(response);
    if (!structureResult.isValid) {
      structureValid = false;
      report.push('❌ API Response Structure Validation Failed:');
      structureResult.errors?.forEach(error => {
        report.push(`   - ${error}`);
      });
    } else {
      report.push('✅ API Response Structure Valid');
    }

    // Validate each product
    if (response.data && Array.isArray(response.data)) {
      const productErrors: string[] = [];
      const criticalErrors: string[] = [];

      response.data.forEach((product: any, index: number) => {
        // Full product validation
        const productResult = this.validateProduct(product);
        if (!productResult.isValid) {
          productResult.errors?.forEach(error => {
            productErrors.push(`Product[${index}]: ${error}`);
          });
        }

        // Critical fields validation
        const criticalResult = this.validateCriticalFields(product);
        if (!criticalResult.isValid) {
          criticalFieldsValid = false;
          criticalResult.errors?.forEach(error => {
            criticalErrors.push(`Product[${index}]: ${error}`);
          });
        }
      });

      if (productErrors.length > 0) {
        report.push('⚠️  Product Structure Warnings:');
        productErrors.slice(0, 5).forEach(error => {
          report.push(`   - ${error}`);
        });
        if (productErrors.length > 5) {
          report.push(`   ... and ${productErrors.length - 5} more`);
        }
      }

      if (criticalErrors.length > 0) {
        report.push('❌ Critical Fields Validation Failed:');
        criticalErrors.forEach(error => {
          report.push(`   - ${error}`);
        });
      } else {
        report.push('✅ All Critical Fields Present');
      }
    }

    return {
      isValid: structureValid && criticalFieldsValid,
      structureValid,
      criticalFieldsValid,
      report
    };
  }

  /**
   * Logs validation report to console
   */
  static logValidationReport(response: any): boolean {
    const validation = this.validateWithReport(response);
    
    console.log('\n📋 API Response Validation Report:');
    validation.report.forEach(line => console.log(line));
    
    if (!validation.isValid) {
      console.log('\n⚠️  API Contract Changed! The webshop API structure has changed and may require code updates.');
    }
    
    return validation.isValid;
  }
}