export interface Price {
  amount: number;
  amountRelevant: number;
  amountRelevantDisplay: string;
  bottleDeposit: number;
  bottleDepositDisplay: string;
  comparison: number;
  comparisonDisplay: string;
  currencyCode: string;
  currencySymbol: string;
  perUnit: string | null;
  perUnitDisplay: string | null;
  wasPriceDisplay: string | null;
  additionalInfo: string | null;
}

export interface Category {
  id: string;
  name: string;
  urlSlugText: string;
}

export interface Asset {
  url: string;
  maxWidth: number;
  maxHeight: number;
  mimeType: string;
  assetType: string;
  alt: string | null;
  displayName: string | null;
}

export interface Product {
  sku: string;
  name: string;
  brandName: string;
  urlSlugText: string;
  ageRestriction: string | null;
  alcohol: boolean | null;
  discontinued: boolean;
  discontinuedNote: string | null;
  notForSale: boolean;
  notForSaleReason: string | null;
  quantityMin: number;
  quantityMax: number;
  quantityInterval: number;
  quantityDefault: number;
  quantityUnit: string;
  weightType: string;
  sellingSize: string;
  energyClass: string | null;
  onSaleDateDisplay: string | null;
  price: Price;
  countryExtensions: any | null;
  categories: Category[];
  assets: Asset[];
}

export interface ApiResponse {
  meta: {
    totalCount: number;
    currentPage: number;
    pageSize: number;
    [key: string]: any;
  };
  data: Product[];
}

export interface CategoryConfig {
  id: string;
  category: string;
  urlSlugText: string;
}

export interface ProductCsv {
  sku: string;
  name: string;
  brandName: string;
  sellingSize: string;
  currency: string;
  price: number;
}

export interface AldiConfig {
  baseUrl: string;
  currency: string;
  serviceType: string;
  sort: string;
  testVariant: string;
  servicePoint: string;
  getNotForSaleProducts: number;
}