import { ApiResponse } from '../types';
import ora from 'ora';

function getRandomDelay(): number {
  return Math.floor(Math.random() * (10000 - 2000 + 1) + 2000);
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export async function delayWithCountdown(ms: number): Promise<void> {
  const seconds = ms / 1000;
  const spinner = ora({
    text: `Rate limiting: waiting ${seconds.toFixed(1)}s`,
    color: 'yellow',
    spinner: 'dots'
  }).start();

  const startTime = Date.now();
  const interval = setInterval(() => {
    const elapsed = Date.now() - startTime;
    const remaining = Math.max(0, (ms - elapsed) / 1000);
    spinner.text = `Rate limiting: waiting ${remaining.toFixed(1)}s`;
  }, 100);

  await delay(ms);
  clearInterval(interval);
  spinner.succeed('Rate limit delay complete');
}

export async function fetchWithRateLimit(url: string, showProgress: boolean = true): Promise<ApiResponse> {
  // Dynamic import for ES module
  const { gotScraping } = await import('got-scraping');
  
  const delayMs = getRandomDelay();
  
  if (showProgress) {
    await delayWithCountdown(delayMs);
  } else {
    await delay(delayMs);
  }

  const fetchSpinner = showProgress ? ora({
    text: 'Fetching data...',
    color: 'cyan',
    spinner: 'dots'
  }).start() : null;

  try {
    const response = await gotScraping({
      url,
      responseType: 'json',
      timeout: {
        request: 30000
      },
      retry: {
        limit: 3,
        methods: ['GET']
      }
    });

    if (fetchSpinner) {
      fetchSpinner.succeed('Data fetched successfully');
    }

    return response.body as ApiResponse;
  } catch (error) {
    if (fetchSpinner) {
      fetchSpinner.fail('Failed to fetch data');
    }
    throw error;
  }
}