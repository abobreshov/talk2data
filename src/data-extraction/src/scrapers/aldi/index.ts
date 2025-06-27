import * as dotenv from 'dotenv';
import chalk from 'chalk';
import { AldiConfig, CategoryConfig, Product, ProductCsv } from '../../types';
import { fetchWithRateLimit } from '../../utils/http';
import { readCategories, writeProducts } from '../../utils/csv';
import { displayBanner, displayConfig, ProgressManager, ScrapingStats, displaySummary, formatProductCount } from '../../utils/display';
import { ResponseValidator } from '../../utils/validation';
import { ValidationDisplay } from '../../utils/validation-display';
import * as path from 'path';

dotenv.config();

export class AldiScraper {
  private readonly config: AldiConfig;
  private products: ProductCsv[] = [];
  private uniqueSkus: Set<string> = new Set();
  private duplicatesFound: number = 0;
  private readonly progressManager: ProgressManager;
  private readonly stats: ScrapingStats;

  constructor() {
    this.config = {
      baseUrl: process.env.ALDI_BASE_URL || '',
      currency: process.env.ALDI_CURRENCY || '',
      serviceType: process.env.ALDI_SERVICE_TYPE || '',
      sort: process.env.ALDI_SORT || '',
      testVariant: process.env.ALDI_TEST_VARIANT || '',
      servicePoint: process.env.ALDI_SERVICE_POINT || '',
      getNotForSaleProducts: parseInt(process.env.ALDI_GET_NOT_FOR_SALE_PRODUCTS || '0')
    };

    this.validateConfig();
    this.progressManager = new ProgressManager();
    this.stats = {
      totalCategories: 0,
      completedCategories: 0,
      totalProducts: 0,
      currentCategory: '',
      startTime: new Date()
    };
  }

  private validateConfig(): void {
    const requiredFields = ['baseUrl', 'currency', 'serviceType', 'sort', 'testVariant', 'servicePoint'];
    const missingFields = requiredFields.filter(field => !this.config[field as keyof AldiConfig]);
    
    if (missingFields.length > 0) {
      throw new Error(`Missing required configuration fields: ${missingFields.join(', ')}`);
    }
  }

  private async showRateLimitDelay(): Promise<void> {
    const delayMs = Math.floor(Math.random() * (10000 - 2000 + 1) + 2000);
    const seconds = delayMs / 1000;
    this.progressManager.showSpinner(`Rate limiting: waiting ${seconds.toFixed(1)}s`);
    
    const startTime = Date.now();
    const interval = setInterval(() => {
      const elapsed = Date.now() - startTime;
      const remaining = Math.max(0, (delayMs - elapsed) / 1000);
      this.progressManager.updateSpinner(`Rate limiting: waiting ${remaining.toFixed(1)}s`);
    }, 100);
    
    await new Promise(resolve => setTimeout(resolve, delayMs));
    clearInterval(interval);
    this.progressManager.stopSpinner(true, 'Rate limit delay complete');
  }

  private buildUrl(categoryId: string, offset: number, limit: number = 30): string {
    const params = new URLSearchParams({
      currency: this.config.currency,
      serviceType: this.config.serviceType,
      categoryKey: categoryId,
      limit: limit.toString(),
      offset: offset.toString(),
      getNotForSaleProducts: this.config.getNotForSaleProducts.toString(),
      sort: this.config.sort,
      testVariant: this.config.testVariant,
      servicePoint: this.config.servicePoint
    });

    return `${this.config.baseUrl}?${params.toString()}`;
  }

  private transformProduct(product: Product): ProductCsv {
    return {
      sku: product.sku,
      name: product.name,
      brandName: product.brandName,
      sellingSize: product.sellingSize,
      currency: product.price.currencySymbol,
      price: product.price.amount
    };
  }

  private async fetchCategoryProducts(category: CategoryConfig, categoryIndex: number): Promise<ProductCsv[]> {
    const categoryProducts: ProductCsv[] = [];
    let offset = 0;
    const limit = 30;
    let hasMore = true;
    let firstResponse = true;
    let pageNumber = 1;
    let categoryDuplicates = 0;
    let totalPages = 0;
    let totalProducts = 0;

    this.stats.currentCategory = category.category;
    
    console.log(chalk.bold.cyan(`\n[${categoryIndex + 1}/${this.stats.totalCategories}] Processing: ${category.category}`));
    console.log(chalk.gray(`    Category ID: ${category.id}`));
    console.log(chalk.gray(`    URL Slug: ${category.urlSlugText}`));

    while (hasMore) {
      try {
        const url = this.buildUrl(category.id, offset, limit);
        
        // Show rate limit timing before each request (except the first)
        if (pageNumber > 1) {
          await this.showRateLimitDelay();
        }
        
        // Show pagination info only if multiple pages
        if (totalPages > 1) {
          console.log(chalk.yellow(`\n    📄 Page ${pageNumber}/${totalPages}: Fetching products ${offset + 1}-${Math.min(offset + limit, totalPages * limit)}...`));
        }
        
        const response = await fetchWithRateLimit(url, false);

        // Validate the first response of each category
        if (firstResponse && categoryIndex === 0) {
          const validation = ResponseValidator.validateWithReport(response);
          if (!validation.isValid) {
            console.log(chalk.yellow('\n⚠️  API Response Validation Warning:'));
            ValidationDisplay.logColoredValidationReport(response);
            
            if (!validation.criticalFieldsValid) {
              throw new Error('Critical fields missing in API response. Cannot continue.');
            }
          }
          firstResponse = false;
        }

        // Calculate total pages if available
        if (response.meta && response.meta.pagination && response.meta.pagination.totalCount && totalPages === 0) {
          totalPages = Math.ceil(response.meta.pagination.totalCount / limit);
          totalProducts = response.meta.pagination.totalCount;
        }

        if (response.data && response.data.length > 0) {
          const transformedProducts = response.data.map(product => this.transformProduct(product));
          
          // Filter duplicates
          const newProducts: ProductCsv[] = [];
          let pageDuplicates = 0;
          
          for (const product of transformedProducts) {
            if (!this.uniqueSkus.has(product.sku)) {
              this.uniqueSkus.add(product.sku);
              newProducts.push(product);
            } else {
              pageDuplicates++;
              categoryDuplicates++;
              this.duplicatesFound++;
            }
          }
          
          categoryProducts.push(...newProducts);
          
          // Show page results only if multiple pages
          if (totalPages > 1) {
            if (pageDuplicates > 0) {
              console.log(chalk.green(`    ✓ Page ${pageNumber}: ${response.data.length} products (${chalk.yellow(`${pageDuplicates} duplicates removed`)})`));
            } else {
              console.log(chalk.green(`    ✓ Page ${pageNumber}: ${response.data.length} products`));
            }
          }
          
          // Show progress info in console
          if (totalProducts > 0) {
            const percentage = Math.round((categoryProducts.length / totalProducts) * 100);
            console.log(chalk.gray(`    Progress: ${categoryProducts.length}/${totalProducts} products (${percentage}%)`));
          }

          if (response.data.length < limit) {
            hasMore = false;
          } else {
            offset += limit;
            pageNumber++;
          }
        } else {
          hasMore = false;
          console.log(chalk.gray(`    ✓ No more products found`));
        }
      } catch (error) {
        console.error(chalk.red(`\n❌ Error fetching category ${category.category}:`), error);
        hasMore = false;
      }
    }

    // Show completion message
    console.log(chalk.green(`    ✓ Category complete: ${categoryProducts.length} products fetched`));
    
    // Show completion with duplicate info
    if (categoryDuplicates > 0) {
      console.log(chalk.green(`    ✓ Completed: ${formatProductCount(categoryProducts.length)} unique products`) + 
                  chalk.yellow(` (${categoryDuplicates} duplicates filtered)\n`));
    } else {
      console.log(chalk.green(`    ✓ Completed: ${formatProductCount(categoryProducts.length)} products\n`));
    }
    
    return categoryProducts;
  }

  public async scrape(): Promise<void> {
    try {
      // Display banner and configuration
      displayBanner();
      displayConfig(this.config);
      
      // Initialize stats
      this.stats.startTime = new Date();
      
      // Load categories
      console.log(chalk.bold.yellow('\n📂 Loading categories...'));
      const categoriesPath = path.resolve(__dirname, '../../../aldi/config/product-categories.csv');
      const categories = await readCategories(categoriesPath);
      this.stats.totalCategories = categories.length;
      
      console.log(chalk.green(`✓ Found ${categories.length} categories to process\n`));

      // Process each category
      for (let i = 0; i < categories.length; i++) {
        const categoryProducts = await this.fetchCategoryProducts(categories[i], i);
        this.products.push(...categoryProducts);
        this.stats.completedCategories++;
        this.stats.totalProducts = this.products.length;
      }

      // Stop progress manager
      this.progressManager.stop();

      // Save results
      console.log(chalk.bold.yellow('\n💾 Saving results...'));
      const outputPath = path.resolve(__dirname, '../../../data/products.csv');
      await writeProducts(this.products, outputPath);

      // Display summary
      displaySummary(this.stats, this.duplicatesFound);
    } catch (error) {
      this.progressManager.stop();
      console.error(chalk.red('\n❌ Scraping failed:'), error);
      throw error;
    }
  }
}
