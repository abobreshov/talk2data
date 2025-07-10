# Generating customers.csv with Mockaroo

This guide explains how to generate the `customers.csv` file using [Mockaroo](https://www.mockaroo.com/), a realistic data generator for testing and development.

## Overview

Mockaroo is a web-based tool that generates realistic test data in various formats. We use it to create 1,000 customer records with UK-focused data.

## Schema Configuration

### Step 1: Access Mockaroo
1. Go to [https://www.mockaroo.com/](https://www.mockaroo.com/)
2. You can use the free tier (up to 1,000 rows)

### Step 2: Configure Fields

Set up the following fields in order:

| Field Name | Type | Options |
|------------|------|---------|
| id | Row Number | Start: 1 |
| first_name | First Name | Default |
| last_name | Last Name | Default |
| email | Email Address | Default |
| gender | Gender | Default |
| address | Street Address | Default |
| city | City | Default |
| postcode | Postcode | Country: United Kingdom |

### Step 3: Detailed Field Configuration

1. **id**
   - Type: `Row Number`
   - Start: `1`

2. **first_name**
   - Type: `First Name`
   - Leave default settings

3. **last_name**
   - Type: `Last Name`
   - Leave default settings

4. **email**
   - Type: `Email Address`
   - Leave default settings

5. **gender**
   - Type: `Gender`
   - Leave default settings

6. **address**
   - Type: `Street Address`
   - Leave default settings

7. **city**
   - Type: `City`
   - Leave default settings (or select UK cities if available)

8. **postcode**
   - Type: `Postcode`
   - Country: `United Kingdom`

### Step 4: Generate Data

1. Set **Rows**: `1000`
2. Set **Format**: `CSV`
3. Check **Include header**
4. Click **Download Data**

### Step 5: Save File

Save the downloaded file as `customers.csv` in the `src/data/` directory.

## Alternative: Using Mockaroo API

If you have a Mockaroo API key, you can generate the data programmatically:

```bash
curl "https://api.mockaroo.com/api/generate.csv?key=YOUR_API_KEY&count=1000" \
  -d schema='[
    {"name":"id","type":"Row Number","start":1},
    {"name":"first_name","type":"First Name"},
    {"name":"last_name","type":"Last Name"},
    {"name":"email","type":"Email Address"},
    {"name":"gender","type":"Gender"},
    {"name":"address","type":"Street Address"},
    {"name":"city","type":"City"},
    {"name":"postcode","type":"Postcode","country":"United Kingdom"}
  ]' \
  > src/data/customers.csv
```

## Validation

After generating, verify the file:

```bash
# Check row count (should be 1001 including header)
wc -l src/data/customers.csv

# View first few rows
head -5 src/data/customers.csv

# Check structure
csvstat src/data/customers.csv
```

Expected output format:
```csv
id,first_name,last_name,email,gender,address,city,postcode
1,John,Smith,john.smith@example.com,Male,123 High Street,London,SW1A 1AA
2,Jane,Doe,jane.doe@example.com,Female,456 Park Lane,Manchester,M1 1AE
...
```

## Notes

1. **Email Uniqueness**: Mockaroo generates unique emails by default
2. **UK Postcodes**: Ensure postcode field is set to United Kingdom for realistic UK postcodes
3. **City Distribution**: Cities will be randomly distributed; you can customize this in Mockaroo
4. **Data Privacy**: All generated data is fictional and safe for testing

## Integration with Order Synthesis

The generated `customers.csv` file is used by:
- `01_environment_setup.py` - Validates customer data
- `04_customer_distribution.py` - Creates order schedules
- `11_create_final_database.py` - Loads into final database

The customer records are essential for creating realistic order patterns in the grocery POC system.