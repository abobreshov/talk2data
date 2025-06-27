import { parse, format } from 'fast-csv';
import * as fs from 'fs';
import * as path from 'path';
import { CategoryConfig, ProductCsv } from '../types';

export async function readCategories(filePath: string): Promise<CategoryConfig[]> {
  return new Promise((resolve, reject) => {
    const categories: CategoryConfig[] = [];
    
    fs.createReadStream(filePath)
      .pipe(parse({ headers: true }))
      .on('data', (row: CategoryConfig) => {
        categories.push(row);
      })
      .on('error', reject)
      .on('end', () => {
        resolve(categories);
      });
  });
}

export async function writeProducts(products: ProductCsv[], filePath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(filePath);
    
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const writeStream = fs.createWriteStream(filePath);
    const csvStream = format({ headers: true });

    csvStream.pipe(writeStream);

    writeStream.on('finish', () => {
      resolve();
    });

    writeStream.on('error', reject);
    csvStream.on('error', reject);

    products.forEach(product => {
      csvStream.write(product);
    });

    csvStream.end();
  });
}