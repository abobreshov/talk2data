import chalk from 'chalk';
import { ResponseValidator } from './validation';

export class ValidationDisplay {
  static logColoredValidationReport(response: any): boolean {
    const validation = ResponseValidator.validateWithReport(response);
    
    console.log(chalk.bold.cyan('\n📋 API Response Validation Report:'));
    
    validation.report.forEach(line => {
      if (line.includes('❌')) {
        console.log(chalk.red(line));
      } else if (line.includes('⚠️')) {
        console.log(chalk.yellow(line));
      } else if (line.includes('✅')) {
        console.log(chalk.green(line));
      } else if (line.startsWith('   -')) {
        console.log(chalk.gray(line));
      } else {
        console.log(line);
      }
    });
    
    if (!validation.isValid) {
      console.log(chalk.bold.red('\n⚠️  API Contract Changed! The webshop API structure has changed and may require code updates.'));
    }
    
    return validation.isValid;
  }
}