import * as dotenv from 'dotenv';
import chalk from 'chalk';
import boxen from 'boxen';
import { fetchWithRateLimit } from './http';
import { ValidationDisplay } from './validation-display';
import { AldiConfig } from '../types';

dotenv.config();

async function validateAldiApi() {
  console.log(boxen(
    chalk.bold.white('🔍 Aldi API Contract Validator\n') +
    chalk.gray('This tool validates the Aldi API response structure\n') +
    chalk.gray('to detect any breaking changes'),
    {
      padding: 1,
      margin: 1,
      borderStyle: 'round',
      borderColor: 'blue',
      textAlignment: 'center'
    }
  ));

  const config: AldiConfig = {
    baseUrl: process.env.ALDI_BASE_URL || '',
    currency: process.env.ALDI_CURRENCY || '',
    serviceType: process.env.ALDI_SERVICE_TYPE || '',
    sort: process.env.ALDI_SORT || '',
    testVariant: process.env.ALDI_TEST_VARIANT || '',
    servicePoint: process.env.ALDI_SERVICE_POINT || '',
    getNotForSaleProducts: parseInt(process.env.ALDI_GET_NOT_FOR_SALE_PRODUCTS || '0')
  };

  // Test with a small request
  const params = new URLSearchParams({
    currency: config.currency,
    serviceType: config.serviceType,
    categoryKey: '1588161416978051001', // Milk category
    limit: '10',
    offset: '0',
    getNotForSaleProducts: config.getNotForSaleProducts.toString(),
    sort: config.sort,
    testVariant: config.testVariant,
    servicePoint: config.servicePoint
  });

  const testUrl = `${config.baseUrl}?${params.toString()}`;

  try {
    console.log(chalk.yellow('\n🔄 Fetching sample data from Aldi API...'));
    console.log(chalk.gray(`URL: ${testUrl}\n`));

    const response = await fetchWithRateLimit(testUrl);
    
    // Log basic response info
    console.log(chalk.cyan('📊 Response Summary:'));
    console.log(chalk.gray(`  • Total products in category: ${response.meta?.totalCount || 0}`));
    console.log(chalk.gray(`  • Products returned: ${response.data?.length || 0}\n`));

    // Perform validation
    const isValid = ValidationDisplay.logColoredValidationReport(response);

    if (isValid) {
      console.log(boxen(
        chalk.bold.green('✅ API Contract Valid\n') +
        chalk.white('The Aldi API is returning data in the expected format.\n') +
        chalk.white('Your scraper should work correctly.'),
        {
          padding: 1,
          margin: 1,
          borderStyle: 'double',
          borderColor: 'green',
          textAlignment: 'center'
        }
      ));
    } else {
      console.log(boxen(
        chalk.bold.red('❌ API Contract Changed\n') +
        chalk.white('The Aldi API structure has changed!\n') +
        chalk.white('Please review the errors above and update the code accordingly.'),
        {
          padding: 1,
          margin: 1,
          borderStyle: 'double',
          borderColor: 'red',
          textAlignment: 'center'
        }
      ));

      // Sample a product if available for debugging
      if (response.data && response.data.length > 0) {
        console.log(chalk.yellow('\n📝 Sample Product Structure:'));
        console.log(JSON.stringify(response.data[0], null, 2));
      }
    }

    process.exit(isValid ? 0 : 1);
  } catch (error) {
    console.error(chalk.red('\n❌ Failed to validate API:'), error);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  validateAldiApi();
}