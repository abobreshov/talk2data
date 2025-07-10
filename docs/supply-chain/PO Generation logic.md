The core process revolves around generating a "base order" for a specific Stock Keeping Unit (SKU) within an identified "ordering opportunity". This involves projecting future stock levels, identifying deficits (rawNeed), and forecasting demand (totalDemand) over a defined period, ultimately leading to a recommended order quantity for that SKU.

# 1\. Input Data Examples

The system relies on various data points to initiate and execute the order generation process:

**SKU Identification:**  
      Sku ID : Item number (e.g., 101)34.  
    
**Ordering Opportunities:**

* orderDate: The date the order is placed (e.g., 2024-03-01).  
* atpDate(firstDelivery): The date the ordered stock is expected to arrive (e.g., 2024-03-05).  
* nextAtpDate(nextDelivery): Represents the delivery date of the next ordering opportunity for this SKU (e.g., 2024-03-12).  
* PurgeDate: The date by which perishable stock needs to be removed ContingencyDaysInAdvance: Days before the order date for contingency calculations (e.g., 2, default 1).  
* ContingencyDaysOfDemand: Days of demand for contingency calculations (e.g., 3, default 7).


**Future Purge for each SKU** (e.g. 10, 2024-09-09)

**System Parameters:**  
    stockProjectionDays: The total number of days to project stock for.

The process flows through several key stages to determine the rawNeed, totalDemand, and ultimately the recommended order quantity.

# Stage 1: Opportunity Identification and SKU Initialisation

1. Identify Ordering Opportunities: This provides a map of orderDate to deliveryDate.

2. Initialise Variables: 

   get SKU data required for the calculation List of data that has the following information for each delivery date in future

           \- sales in the basket (ITB)

           \- sales forecast

           \- opening stock today

           \- get inbound deliveries

   

# Stage 2: Generate Stock Projections ()

1.  Calculate Projection Duration: The system determines **stockProjectionDays** from the orderDate (or creationDate) to the PurgeDate.


2.  Generate StockForecast List: create a list of StockForecast objects, each representing the projected stock level (stockProjection), sales forecast (salesForecast), inbound quantity, and purge quantity for a specific date.  
   1. Starting Stock: currentStock is calculated from future Purge entries that have not yet occurred.  
      Example: If futurePurge on 2024-03-01 totals 70 units, currentStock begins at 70\.

                        availableStock=purge data

2. iterate through each day (currentDate) for the **stockProjectionDays** and define the following and calculate availableStock in the end of the period:  
   * inboundQuantity: Incoming stock deliveries are added to availableStock.  
     * outboundQuantity: Sales forecast is consumed from available stock ) \[depending on purge\]  
     * purgeQuantity: Stock that has passed its purge date is removed.  
     * endingStock: Calculated as currentStock \+ inboundQuantity \- outboundQuantity \- purgeQuantity.

 

| Day | Inbound Stock (Qty, Purge Date) | Sales (Qty) | Available Batches (Before Sales) |
| ----- | ----- | ----- | ----- |
| 1 | 50 (purge in 3 days) | 60 | A: 30 (exp. today), B: 40 (tomorrow), C: 80 (in 5 days) |
| 2 | 20 (purge in 4 days) | 70 | B: 10 (leftover), C: 80, D: 50 (from Day 1 inbound) |
| 3 | 10 (purge in 5 days) | 120 | C: 30 (leftover), D: 20 (leftover), E: 20 (from Day 2\) |

| Day | Starting Stock | Inbound | Outbound | Purge | Ending Stock | Note |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| 1 | 100 | \+50 | \-60 | \-0 | **90** | Batch A (30) & B (30) used, no purged batch left |
| 2 | 90 | \+20 | \-70 | \-10 | **30** | Batch B expired (10 purged), Sales consume C (50) & D (20) |
| 3 | 30 | \+10 | \-120 | \-10 | **\-90** | Sales exceed all available stock; purged D (10), stock goes negative |

\[Current Stock: 100\]  
          |  
          | \+ Inbound: 50  
          v  
\[Stock After Inbound: 150\]  
          |  
          | \- Outbound (Sales): 60  
          |   • Future Purge A (30 units, expires today)  
          |   • Future Purge B (30 units, expires tomorrow)  
          v  
\[AvailableStock Consumed\]  
          |  
          | \- Purge: 0  
          |   • Batch A already used, no leftover expired  
          v  
\[Ending Stock: 90\]

# Stage 3: Calculate and (within )

Define the Opportunity Period: The key "opportunity period" for these calculations is the time window between the current delivery date ( 2024-03-05) and the next delivery date (2024-03-12)  as defined in the SkuOrderingCalendar   
Iterate StockForecast Objects: The system loops through each StockForecast (sp2) generated in Stage 2\.

####        rawNeed Calculation:

rawNeed accumulates the negative stockProjection for any StockForecast that falls within the opportunity period (2024-03-05 to 2024-03-12).

rawNeed will be 0D if no StockForecast object within the defined opportunity period shows a projected stock deficit 

#### totalDemand Calculation:

totalDemand sums the salesForecast (outboundQuantity) for any StockForecast that falls within the same opportunity period. This represents the anticipated sales for which the current order is intended to provide coverage.

### **Assumptions for the Example**

* **Order Date**: 2025-06-20

* **ATP Date (first delivery)**: 2025-06-21

* **Next ATP Date (OrderToAtpBucket)**: 2025-06-25

* **Purge Date**: 2025-06-28

* **Contingency Days in Advance**: 1 (2025-06-21)

* **Contingency Days of Demand**: 3 (2025-06-24)

 So the **Contingency Date Range** \= 2025-06-21 to 2025-06-24  
 And **Max Cover Date** \= 2025-07-02  
 And **Contingency Raw Need Max Date** \= 2025-07-05

| Date | Stock Projection(previous stage) | Sales Forecast | Actual Purge |
| ----- | ----- | ----- | ----- |
| 2025-06-21 | 80 | 10 | 0 |
| 2025-06-22 | 40 | 12 | 0 |
| 2025-06-23 | \-10 | 15 | 5 |
| 2025-06-24 | \-30 | 10 | 10 |
| 2025-06-25 | \-50 | 20 | 0 |
| 2025-06-26 | 10 | 25 | 2 |
| 2025-06-27 | \-20 | 18 | 0 |
| 2025-06-28 | \-40 | 12 | 0 |

| Field | How it’s calculated | Value |
| ----- | ----- | ----- |
| **rawNeed** | Sum of negative projections from ATP (2025-06-21) to next ATP (2025-06-25): \-10 \+ \-30 \= \-40 | **40** |
| **totalDemand** | Sum of sales forecast from ATP to next ATP: 10 \+ 12 \+ 15 \+ 10 | **47** |
| **demandTillPurgeDate** | From next ATP (2025-06-25) to purge (2025-06-28): 20 \+ 25 \+ 18 \+ 12 | **75** |
| **rawNeedTillPurge** | Negative projections from next ATP to purge: \-50 \+ \-20 \+ \-40 | **110** |
| **contingencyForecastSales** | Sales forecast from 2025-06-21 to 2025-06-24: 10 \+ 12 \+ 15 \+ 10 | **47** |
| **contingencyRawNeed** | Same dates, only where projection \< 0: \-10 \+ \-30 | **40** |
| **finalStock** | On next ATP date (2025-06-25), stock \= max(0, \-50) \+ actualPurge \= 0 \+ 0 | **0** |

# Example

Let's assume **today is `2025-06-20`**, and we'll generate sample data for **7 days** for SKU `SKU12345`.

---

### **📅 `sku_ordering_calendar`**

| sku\_id | order\_date | atp\_delivery\_date | next\_atp\_delivery\_date | purge\_date | contingency\_days\_in\_advance | contingency\_days\_of\_demand |
| ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| SKU12345 | 2025-06-20 | 2025-06-22 | 2025-06-24 | 2025-06-27 | 2 | 4 |

---

**Field Explanation**

* **order\_date**: When the order is created (today in this case).

* **atp\_delivery\_date**: The earliest future date when stock can be received.

* **next\_atp\_delivery\_date**: The next ATP window after the first one.

* **purge\_date**: The last date for which forecasts are relevant (e.g., expiry horizon).

* **contingency\_days\_in\_advance**: Number of days before ATP to start calculating contingency demand.

* **contingency\_days\_of\_demand**: The number of days of expected demand to be covered as a buffer.

---

### **📦 `sales_forecast` (starting from today)**

| sku\_id | date | forecast\_qty |
| ----- | ----- | ----- |
| SKU12345 | 2025-06-20 | 18 |
| SKU12345 | 2025-06-21 | 20 |
| SKU12345 | 2025-06-22 | 22 |
| SKU12345 | 2025-06-23 | 19 |
| SKU12345 | 2025-06-24 | 23 |
| SKU12345 | 2025-06-25 | 21 |
| SKU12345 | 2025-06-26 | 20 |

---

### **🚚 `inbound_stock` (incoming deliveries)**

| sku\_id | date | inbound\_qty |
| ----- | ----- | ----- |
| SKU12345 | 2025-06-22 | 15 |

---

### **🗑️ `purge_stock` (QA/expiration removals)**

| sku\_id | purge\_date | purge\_qty |
| :---- | :---- | :---- |
| SKU12345 | 2025-06-23 | 25 |
| SKU12345 | 2025-06-25 | 30 |
| SKU12345 | 2025-06-27 | 20 |

### 

finalStock \= initialStock \+ inbound \- outbound \- (purge \- outbound)

| Date | Calculation | finalStock |
| ----- | ----- | ----- |
| 2025-06-20 | initialStock \= 75 | 75 |
|  | Ending stock \= 75 \- 18 (sales) | 57 |
| 2025-06-21 | Starting stock \= 57 |  |
|  | Ending stock \= 57 \- 20 (sales) | 37 |
| 2025-06-22 | Ending stock \= 37 \+ 15 (inbound) \- 22 (sales) \= 30 | 30 |
| 2025-06-23 | Ending stock \= 30 \- 19 (sales) \- 6 (purge) \= 8 | 5 |
| 2025-06-24 | Ending stock \= 5 \- 23 (sales) \= \-18 | \-18 (shortage) |
| 2025-06-25 | Ending stock \= \-18 \- 21 (sales) \- 2 (purge) \= \-39 | \-28 (shortage) |
| 2025-06-26 | Ending stock \= \-39 \- 20 (sales) \= \-59 | \-59 (shortage) |

#### Base order

| Date | Stock Projection (endingStock) | Within Contingency Dates? | Comments |
| ----- | ----- | ----- | ----- |
| 2025-06-20 | 75 | No | Before contingency start |
| 2025-06-21 | 57 | No | Before contingency start |
| 2025-06-22 | 30 | Yes | Start of contingency |
| 2025-06-23 | 5 | Yes | Within contingency |
| 2025-06-24 | \-18 | Yes | Within contingency |
| 2025-06-25 | \-28 | Yes | End of contingency |
| 2025-06-26 | \-59 | Yes | After contingency end, but inside purge date |

| Field | How it’s calculated | Value |
| ----- | ----- | ----- |
| **rawNeed** | Sum of negative projections from ATP (2025-06-22) to next ATP (2025-06-24):  | **18** |
| **totalDemand** | Sum of sales forecast from ATP (2025-06-22) to next ATP (2025-06-24) | **64** |
| **demandTillPurgeDate** | From next ATP (2025-06-24) to purge (2025-06-27) | **64** |
| **rawNeedTillPurge** | Negative projections from next ATP (2025-06-24) to purge (2025-06-27):  | **105** |
| **contingencyForecastSales** | Sales forecast from 2025-06-22 to 2025-06-26 | **105** |
| **contingencyRawNeed** | Same dates, only where projection \< 0: \-10 \+ \-30 | **105** |
| **finalStock** | On next ATP date (2025-06-26), stock \= max(0, \-105) \+ actualPurge \= 0 \+ 0  | **0** |

