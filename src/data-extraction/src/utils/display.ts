import chalk from 'chalk';
import boxen from 'boxen';
import ora from 'ora';

export interface ScrapingStats {
  totalCategories: number;
  completedCategories: number;
  totalProducts: number;
  currentCategory: string;
  startTime: Date;
}

export function displayBanner(): void {
  const banner = boxen(
    chalk.bold.white('🛒 Grocery Data Extraction Tool\n') +
    chalk.gray('Version: ') + chalk.cyan('1.0.0\n') +
    chalk.gray('Store: ') + chalk.cyan('Aldi UK\n') +
    chalk.gray('API: ') + chalk.cyan('https://api.aldi.co.uk/v3/product-search'),
    {
      padding: 1,
      margin: 1,
      borderStyle: 'round',
      borderColor: 'cyan',
      textAlignment: 'center'
    }
  );
  console.log(banner);
}

export function displayConfig(config: any): void {
  console.log(chalk.bold.yellow('\n📋 Configuration:'));
  console.log(chalk.gray('  • Currency: ') + chalk.white(config.currency));
  console.log(chalk.gray('  • Service Type: ') + chalk.white(config.serviceType));
  console.log(chalk.gray('  • Service Point: ') + chalk.white(config.servicePoint));
  console.log(chalk.gray('  • Rate Limit: ') + chalk.white('2-10 seconds between requests'));
  console.log(chalk.gray('  • Page Size: ') + chalk.white('30 products per request'));
}

export class ProgressManager {
  private spinner: any;
  
  constructor() {
    // Removed multi-bar to avoid console glitching
  }

  showSpinner(text: string): any {
    this.spinner = ora({
      text,
      color: 'cyan',
      spinner: 'dots'
    }).start();
    return this.spinner;
  }

  updateSpinner(text: string): void {
    if (this.spinner) {
      this.spinner.text = text;
    }
  }

  stopSpinner(success: boolean = true, text?: string): void {
    if (this.spinner) {
      if (success) {
        this.spinner.succeed(text);
      } else {
        this.spinner.fail(text);
      }
    }
  }

  stop(): void {
    if (this.spinner) {
      this.spinner.stop();
    }
  }
}

export function displaySummary(stats: ScrapingStats, duplicatesFound: number = 0): void {
  const elapsedTime = Math.round((Date.now() - stats.startTime.getTime()) / 1000);
  const minutes = Math.floor(elapsedTime / 60);
  const seconds = elapsedTime % 60;
  
  let summaryText = chalk.bold.green('✅ Scraping Complete!\n\n') +
    chalk.white('📊 Summary:\n') +
    chalk.gray('  • Categories processed: ') + chalk.cyan(stats.completedCategories) + '\n' +
    chalk.gray('  • Total unique products: ') + chalk.cyan(stats.totalProducts) + '\n';
  
  if (duplicatesFound > 0) {
    summaryText += chalk.gray('  • Duplicates removed: ') + chalk.yellow(duplicatesFound) + '\n';
  }
  
  summaryText += chalk.gray('  • Time elapsed: ') + chalk.cyan(`${minutes}m ${seconds}s`) + '\n' +
    chalk.gray('  • Output file: ') + chalk.cyan('data/products.csv');
  
  const summary = boxen(
    summaryText,
    {
      padding: 1,
      margin: 1,
      borderStyle: 'double',
      borderColor: 'green',
      textAlignment: 'left'
    }
  );
  console.log(summary);
}

export function displayError(error: Error): void {
  const errorBox = boxen(
    chalk.bold.red('❌ Error!\n\n') +
    chalk.white(error.message),
    {
      padding: 1,
      margin: 1,
      borderStyle: 'round',
      borderColor: 'red',
      textAlignment: 'left'
    }
  );
  console.log(errorBox);
}

export function formatProductCount(count: number): string {
  return count.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}