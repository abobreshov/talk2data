import Joi from 'joi';

// Price schema - based on the example provided
export const priceSchema = Joi.object({
  amount: Joi.number().required(),
  amountRelevant: Joi.number().required(),
  amountRelevantDisplay: Joi.string().required().pattern(/^£\d+\.\d{2}$/),
  bottleDeposit: Joi.number().required(),
  bottleDepositDisplay: Joi.string().required().pattern(/^£\d+\.\d{2}$/),
  comparison: Joi.number().allow(null).required(),
  comparisonDisplay: Joi.string().allow(null).required(),
  currencyCode: Joi.string().valid('GBP').required(),
  currencySymbol: Joi.string().valid('£').required(),
  perUnit: Joi.string().allow(null),
  perUnitDisplay: Joi.string().allow(null),
  wasPriceDisplay: Joi.string().allow(null),
  additionalInfo: Joi.string().allow(null)
}).required();

// Category schema
export const categorySchema = Joi.object({
  id: Joi.string().required(),
  name: Joi.string().required(),
  urlSlugText: Joi.string().required()
});

// Asset schema
export const assetSchema = Joi.object({
  url: Joi.string().required(), // URLs may contain template variables
  maxWidth: Joi.number().required(),
  maxHeight: Joi.number().required(),
  mimeType: Joi.string().required(),
  assetType: Joi.string().required(),
  alt: Joi.string().allow(null),
  displayName: Joi.string().allow(null)
});

// Product schema - based on the example provided
export const productSchema = Joi.object({
  sku: Joi.string().required(),
  name: Joi.string().required(),
  brandName: Joi.string().required(),
  urlSlugText: Joi.string().required(),
  ageRestriction: Joi.string().allow(null),
  alcohol: Joi.boolean().allow(null),
  discontinued: Joi.boolean().required(),
  discontinuedNote: Joi.string().allow(null),
  notForSale: Joi.boolean().required(),
  notForSaleReason: Joi.string().allow(null),
  quantityMin: Joi.number().integer().positive().required(),
  quantityMax: Joi.number().integer().positive().required(),
  quantityInterval: Joi.number().integer().positive().required(),
  quantityDefault: Joi.number().integer().positive().required(),
  quantityUnit: Joi.string().required(),
  weightType: Joi.string().required(),
  sellingSize: Joi.string().required(),
  energyClass: Joi.string().allow(null),
  onSaleDateDisplay: Joi.string().allow(null),
  price: priceSchema.required(),
  countryExtensions: Joi.any().allow(null),
  categories: Joi.array().items(categorySchema).required(),
  assets: Joi.array().items(assetSchema).required()
}).required();

// Pagination schema
export const paginationSchema = Joi.object({
  offset: Joi.number().integer().min(0).required(),
  limit: Joi.number().integer().positive().required(),
  totalCount: Joi.number().integer().min(0).required()
}).unknown(true);

// Meta schema for pagination info
export const metaSchema = Joi.object({
  pagination: paginationSchema.required(),
  spellingSuggestion: Joi.any().allow(null),
  pinned: Joi.array(),
  keywordRedirect: Joi.any().allow(null),
  debug: Joi.any().allow(null),
  facets: Joi.array(),
  sort: Joi.array()
}).unknown(true); // Allow additional fields in meta

// Complete API response schema
export const apiResponseSchema = Joi.object({
  meta: metaSchema.required(),
  data: Joi.array().items(productSchema).required()
}).required();

// Schema for critical fields that our application depends on
export const criticalFieldsSchema = Joi.object({
  sku: Joi.string().required(),
  name: Joi.string().required(),
  brandName: Joi.string().required(),
  sellingSize: Joi.string().required(),
  price: Joi.object({
    amountRelevantDisplay: Joi.string().pattern(/^£\d+\.\d{2}$/).required()
  }).unknown(true).required()
}).unknown(true);