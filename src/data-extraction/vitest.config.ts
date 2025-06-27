import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
    exclude: ['node_modules', 'dist', 'coverage'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov', 'html'],
      exclude: [
        'node_modules/',
        'src/**/*.d.ts',
        'src/index.ts',
        'tests/',
        'vitest.config.ts',
      ],
    },
  },
  resolve: {
    alias: {
      '@': '/src',
    },
  },
});