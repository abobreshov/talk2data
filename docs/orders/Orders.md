## Orders data schema

| Name | Type | Description |
| :---- | :---- | :---- |
| orderId | STRING | From Data Dictionary: The ID for the order. |
| orderCreatedDateTime | TIMESTAMP | From Data Dictionary: The date and time when the customer first placed the order. |
| fulfillmentId | STRING | From Data Dictionary: The ID for the fulfillment task used to fulfill the customer's order. |
| bannerId | STRING | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=bannerId) |
| retailerBannerId | STRING | From Data Dictionary: The external ID for the banner that the order belongs to. |
| regionId | STRING | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=regionId) |
| retailerRegionId | STRING | From Data Dictionary: The external ID for the region that the order belongs to. |
| retailerRegionName | STRING | From Data Dictionary: Your name for the region that the order belongs to. |
| \_fulfillment\_fulfillmentId | STRING | \- |
| routeId | STRING | From Data Dictionary: The ID for the route the order belongs to. You can use this information along with the \<code\>routeName\</code\> to uniquely identify a route. |
| routeName | STRING | From Data Dictionary: A readable name for the route the order belongs to. You can use this information along with the \<code\>routeId\</code\> to uniquely identify a route. |
| orderEvent | STRING | From Data Dictionary: The event that changed the status of the order. Enumeration: PICK, CHECKOUT, CANCEL, EDIT, DELIVERY |
| orderStatus | STRING | From Data Dictionary: The status of the order. Enumeration: CANCELLED, DELIVERED, PICKED, FUTURE |
| fulfillmentStatus | STRING | \- |
| orderStatusDateTime | TIMESTAMP | From Data Dictionary: The date and time when the order status changed. |
| fulfillmentStatusDateTime | TIMESTAMP | \- |
| editOrderCutoffDateTime | TIMESTAMP | From Data Dictionary: The latest time at which a customer can edit their order. |
| bookedDeliveryStartDateTime | TIMESTAMP | From Data Dictionary: The start time of the delivery slot booked by the customer. The actual delivery may occur outside the customer’s chosen slot. |
| bookedDeliveryEndDateTime | TIMESTAMP | From Data Dictionary: The end time of the delivery slot booked by the customer. The actual delivery may occur outside the customer’s chosen slot. |
| deliveryMethod | STRING | From Data Dictionary: The delivery method chosen by the customer. Enumeration: BRANDED\_VAN, C\_N\_C |
| cancelReason | STRING | From Data Dictionary: The reason why the order was canceled. This is only displayed if the order was canceled. Enumeration: FAILED\_PICK, CUST\_ERROR, FAILED\_COLLECTION, FRAUD, CANCELLED\_BY\_CUSTOMER, CUST\_NOT\_IN, PAYMENT\_AUTH\_DECLINED, FAILED\_DELIVERY |
| pickingMethod | STRING | From Data Dictionary: The type of picking method used to fulfill the order. Enumeration: CFC, STORE |
| paymentId | STRING | From Data Dictionary: (Deprecated) The unique identifier for the payment associated with the order. |
| \_customer\_customerId | STRING | \- |
| retailerCustomerId | STRING | From Data Dictionary: Your identifier for the customer. |
| retailerFulfillmentSiteId | STRING | From Data Dictionary: Your identifier for the site where the customer order was fulfilled. |
| orderCurrency | STRING | From Data Dictionary: The currency used to place the order. |
| checkoutTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at checkout. Excludes discounts, delivery, and other charges. |
| checkoutTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at checkout, including any discounts. Excludes delivery or other charges. |
| checkoutTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at checkout. Excludes discounts. |
| checkoutTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery and any discounts on charges, as calculated at checkout. |
| checkoutTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts, as calculated at checkout. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| checkoutTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to the order after checkout, including items, promotion codes, charges, and promotions. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the order, after checkout. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax applied to the tax component. |
| taxableAmount | STRING | From Data Dictionary: The total amount associated with the entities to which tax applies, after checkout. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the order, after checkout. |
| pickTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the time of picking. Excludes discounts and delivery or other charges. This is only displayed after the order has been picked. |
| pickTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the time of picking, including any discounts. Excludes delivery or other charges. This is only displayed after the order has been picked. |
| pickTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the time of picking. Excludes discounts. This is only displayed after the order has been picked. |
| pickTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the time of picking. Includes discounts. This is only displayed after the order has been picked. |
| pickTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts, as calculated at the time of picking. This is only displayed after the order has been picked. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| pickTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the order after picking. This is only displayed after the order has been picked. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the order, after picking. This is only displayed after the order has been picked. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax applied to the tax component. |
| taxableAmount | STRING | From Data Dictionary: The total amount associated with the entities to which tax applies, after picking. This is only displayed after the order has been picked. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the order, after picking. This is only displayed after the order has been picked. |
| finalTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the moment of delivery. Excludes discounts and delivery or other charges. This is only displayed after the order has been delivered. |
| finalTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the moment of delivery, including any discounts. Excludes delivery or other charges. This is only displayed after the order has been delivered. |
| finalTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the moment of delivery. Excludes discounts. This is only displayed after the order has been delivered. |
| finalTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery and any discounts on charges. This is only displayed after the order has been delivered. |
| orderLoyaltyValueSpentAmount | STRING | From Data Dictionary: The monetary equivalent of the loyalty units spent. |
| finalTotalReturnsTotalAmount | RECORD | From Data Dictionary: The total gain to the customer for all returns. This is only displayed after the order has been delivered. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| finalTotalReturnsTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to the buybacks amount, including boughtback bags. This is only displayed after the order has been delivered. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to buybacks. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with buybacks. |
| taxableAmount | STRING | From Data Dictionary: The total amount associated with the entities to which tax applies. This is only displayed after the order has been delivered. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the buybacks amount. This is only displayed after the order has been delivered. |
| finalTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts. This is only displayed after the order has been delivered. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| finalTotalFinalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the final order amount. This is only displayed after the order has been delivered. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the order, after the delivery. This is only displayed after the order has been delivered. Enumeration: A, B, etc., Z |
| taxRate | STRING | \- |
| taxableAmount | STRING | From Data Dictionary: The total amount associated with the entities to which tax applies. This is only displayed after the order has been delivered. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the final order amount. This is only displayed after the order has been delivered. |
| deliveryChargeUndiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for delivery or customer collection for their order, excluding any discounts. |
| deliveryChargeDiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for delivery of their order, including any discounts. |
| deliveryChargeTaxedAmount | RECORD | From Data Dictionary: The sum of all the taxes applied to the delivery charge. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| deliveryChargePromotions | REPEATED RECORD | From Data Dictionary: Information about the promotion applied to the delivery charge for the order. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the delivery reward promotion. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the delivery reward promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the discount applied to the delivery charge. |
| deliveryChargeTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the delivery charge for the order. |
| taxCode | STRING | From Data Dictionary: The code of tax associated with the delivery charge. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with the delivery charge amount. |
| taxableAmount | STRING | From Data Dictionary: The proportion of the value that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the delivery charge. |
| preparationCostUndiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for delivery or customer collection for their order, excluding any discounts. |
| preparationCostDiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for delivery of their order, including any discounts. |
| preparationCostTaxedAmount | RECORD | From Data Dictionary: The sum of all the taxes applied to the preparation charge. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| preparationCostPromotions | REPEATED RECORD | From Data Dictionary: Information about the promotion applied to the delivery charge for the order. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the delivery reward promotion. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the delivery reward promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the discount applied to the delivery charge. |
| preparationCostTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the preparation charge for the order. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the preparation charge for the order. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with the preparation charge. |
| taxableAmount | STRING | From Data Dictionary: The proportion of the value that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the preparation charge. |
| carrierBagChargeUndiscountedAmount | STRING | From Data Dictionary: The value of the carrier bag charge to be paid before any promotions or discounts are applied. |
| carrierBagChargeDiscountedAmount | STRING | From Data Dictionary: The value of the carrier bag charge to be paid once promotions and discounts to the charge have been applied. |
| carrierBagChargeTaxedAmount | RECORD | From Data Dictionary: The sum of all the taxes applied to the carrier bag charge. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| carrierBagChargeNumberOfBags | INTEGER | From Data Dictionary: The number of bags for individual bag charge |
| carrierBagChargeTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the carrier bag charge for the order. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the carrier bag charge. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with the carrier bag charge. |
| taxableAmount | STRING | From Data Dictionary: The proportion of the value that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the carrier bag charge. |
| returnedBagsNumber | INTEGER | From Data Dictionary: The number of bags bought back from the customer |
| returnedBagsAmount | RECORD | From Data Dictionary: The value of the bags returned. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| returnedBagsTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to returned bags. This is only displayed after the order has been delivered. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to buybacked bags. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with buybacked bags. |
| taxableAmount | STRING | From Data Dictionary: The total amount associated with the entities to which tax applies. This is only displayed after the order has been delivered. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the buybacked bags amount. This is only displayed after the order has been delivered. |
| items | REPEATED RECORD | From Data Dictionary: Information about the individual items which make up the order. |
| retailerProductId | STRING | From Data Dictionary: The external ID for the item. This may be the item ordered by the customer or one substituted during fulfillment. |
| orderReceiptTemperatureRegime | STRING | From Data Dictionary: The temperature regime for a product as stated on the customer's receipt. |
| picked | STRING | From Data Dictionary: Indicates if the item was picked. If TRUE, the item has been picked and dispatched for delivery; if FALSE, the customer's order has been dispatched for delivery but the item was not picked, or the item has been substituted; if NULL, the order which the item is part of has not yet been dispatched for delivery. This flag is set when the delivery van is marked as dispatched. Enumeration: false, true |
| delivered | STRING | From Data Dictionary: Indicates if the item was delivered. If TRUE, the item has been delivered; if FALSE, the item was not picked, was substituted, was not delivered to the customer, or was rejected by the customer at delivery; if NULL, the order which the item is part of has not been marked as delivered. This flag is usually set automatically when the delivery device is returned to the dispatch site, although Driver managers can also manually mark a delivery route as completed. Enumeration: false, true |
| undiscountedPriceAmount | STRING | From Data Dictionary: The item price before any discounts. |
| discountedPriceAmount | STRING | From Data Dictionary: The item price including any discounts. |
| taxedPriceAmount | RECORD | From Data Dictionary: The sum of all taxes applied to the item. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| itemPromotions | REPEATED RECORD | From Data Dictionary: Information about the promotions applied to each item that was ordered. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the promotion applied to the item. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the promotional discount applied to the item. |
| promotionType | STRING | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=items.itemPromotions.promotionType) |
| itemTaxComponents | REPEATED RECORD | \- |
| taxCode | STRING | \- |
| taxRate | STRING | \- |
| taxableAmount | STRING | \- |
| taxAmount | STRING | \- |
| substitutes | REPEATED RECORD | From Data Dictionary: Information about the individual items which make up the order. This information is only displayed after the substitute items have been picked. |
| retailerProductId | STRING | From Data Dictionary: The external ID for the substitute item. |
| orderReceiptTemperatureRegime | STRING | From Data Dictionary: The temperature regime of the substitute item. This is displayed on the customer's receipt. |
| delivered | STRING | From Data Dictionary: Indicates if the substitute was delivered. If TRUE, the substitute has been delivered; if FALSE the substitute has been adjusted by the Driver. Will be NULL before the substitute is processed by the delivery channel. Enumeration: false, true |
| undiscountedPriceAmount | STRING | From Data Dictionary: Original price of the product before any promotions or discounts are applied. |
| discountedPriceAmount | STRING | From Data Dictionary: Final price of the product after promotions and discounts have been applied. This is the price the customer will pay. |
| taxedPriceAmount | RECORD | From Data Dictionary: The sum of all taxes applied to the substitute. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| itemPromotions | REPEATED RECORD | From Data Dictionary: Information about the promotions applied to each substituted item which makes up the order. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the promotion applied to the substituted item. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the promotional discount applied to the individual substitute as a result of the above promotion. |
| promotionType | STRING | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=items.substitutes.itemPromotions.promotionType) |
| itemTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to each substitute. |
| taxCode | STRING | From Data Dictionary: The code of tax applied to the substitute. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with the substitute. |
| taxableAmount | STRING | From Data Dictionary: The proportion of the value that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the substitute. |
| depositTotalAmount | STRING | From Data Dictionary: Total deposit amount combined with the substitute. |
| deposits | REPEATED RECORD | From Data Dictionary: Information about the deposits associated with each substituted item which makes up the order. |
| retailerProductDepositId | STRING | From Data Dictionary: The external ID for the deposit. |
| priceAmount | RECORD | From Data Dictionary: The sum of all taxes applied to the item. |
| taxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to each substitute's deposit. |
| additionalProductCharge | RECORD | From Data Dictionary: Information about the additional product charge (such as environmental handling charge) associated with each individual substitute of the item which makes up the order. |
| retailerAdditionalProductChargeId | STRING | From Data Dictionary: The external ID of the additional product charge. |
| priceAmount | RECORD | From Data Dictionary: The amount of additional product charges applied to the substitute. |
| taxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to each item's additional product charge. |
| sellerId | STRING | From Data Dictionary: The ID for the seller associated with the substitute item. |
| weight | STRING | \- |
| depositTotalAmount | STRING | From Data Dictionary: Total deposit amount combined with the individual item. |
| deposits | REPEATED RECORD | From Data Dictionary: Information about the deposits associated with each individual item which makes up the order. |
| additionalProductCharge | RECORD | From Data Dictionary: Information about the additional product charge (such as environmental handling charge) associated with each item which makes up the order. |
| timeOfAddingToCart | TIMESTAMP | From Data Dictionary: The date and time when the customer added the item to their cart. |
| sellerId | STRING | From Data Dictionary: The ID for the seller associated with the item. |
| weight | STRING | \- |
| orderPromotions | REPEATED RECORD | From Data Dictionary: Information about the promotions applied to the entire order. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the promotion applied to the order. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the promotion. |
| discountAmount | RECORD | From Data Dictionary: The monetary value of the promotional discount applied to the order. |
| orderPromotionTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to order-level promotions. |
| \_receipts\_orderId | STRING | \- |
| \_createdAt | TIMESTAMP | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=_createdAt) |
| \_scheduledFor | TIMESTAMP | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=_scheduledFor) |
| slaInfo | RECORD | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=slaInfo) |
| orderDetail | RECORD | \- |
| fulfillmentStateHistory | RECORD | \- |
| orderReceiptProductTemperatureRegime | RECORD | \- |
| partitionVersion | INTEGER | \- [Check in Data Dictionary](https://sherlock.devtools.osp.tech/manage-data/dictionary/fields?selected=partitionVersion) |
| \_promotions | REPEATED RECORD | \- |
| promotionId | STRING | \- |
| retailerPromotionId | STRING | \- |
| retailerPromotionCode | STRING | \- |
| type | STRING | \- |
| checkoutTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order as calculated at checkout. |
| pickTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order as calculated at the time of picking. |
| finalTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order. |
| invoiceChargeUndiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer in a business-to-business or business-to-consumer relation when parties settle their payments with invoices, excluding any discounts. |
| invoiceChargeDiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer in a business-to-business or business-to-consumer relation when parties settle their payments with invoices, including any discounts. |
| invoiceChargeTaxedAmount | RECORD | From Data Dictionary: The sum of all the taxes applied to the invoice charge. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| invoiceChargePromotions | REPEATED RECORD | From Data Dictionary: Information about the promotion applied to the invoice charge for the order. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the invoice promotion. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the invoice promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the charge deducted due to the promotion. |
| invoiceChargeTaxComponents | REPEATED RECORD | From Data Dictionary: Details about the specific tax components that are applied to the invoice charge. |
| taxCode | STRING | From Data Dictionary: The tax code of the tax component. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax associated with the invoice charge amount. |
| taxableAmount | STRING | From Data Dictionary: The proportion of the value that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the invoice charge. |
| checkoutTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order as calculated at checkout. |
| pickTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order as calculated at the time of picking. |
| finalTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order at the time of delivery. |
| retailerCustomerAddressId | STRING | From Data Dictionary: Your identifier of the customer's address that was selected for delivery of the order. |
| billingAddressId | STRING | From Data Dictionary: The ID for the billing address used by the customer. |
| platform | STRING | From Data Dictionary: The platform where the order was placed: \<br\>\<code\>WEB\</code\> \- the customer placed the order by using the web version of the online store.\<br\>\<code\>MOBILE\</code\> \- the customer placed the order by using a mobile app version of the online store. \<br\>\<code\>API\</code\> \- an order created for the customer by an external application outside OSP or created automatically by OSP, such as recurring orders. Enumeration: WEB, API, MOBILE |
| tags | REPEATED STRING | From Data Dictionary: The list of tags applied to an order to provide additional metadata. For example, the RECURRING tag is assigned to recurring orders. Enumeration: RECURRING |
| deliveryChargeSellerId | STRING | From Data Dictionary: The ID for the seller associated with the delivery charge. |
| preparationCostSellerId | STRING | From Data Dictionary: The ID for the seller associated with the preparation charge. |
| carrierBagChargeSellerId | STRING | From Data Dictionary: The ID for the seller associated with the carrier bag charge. |
| invoiceChargeSellerId | STRING | From Data Dictionary: The ID for the seller associated with the invoice charge. |
| totalsPerSeller | REPEATED RECORD | From Data Dictionary: The total cost of goods in the customer's order, split by seller and showing the cost after checkout, upon picking, and at delivery. |
| sellerId | STRING | From Data Dictionary: The ID for the seller. |
| checkoutTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at checkout. Excludes discounts and delivery or other charges. |
| checkoutTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at checkout, including any discounts. Excludes delivery or other charges. |
| checkoutTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at checkout. Excludes discounts. |
| checkoutTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery and any discounts on charges, as calculated at checkout. |
| checkoutTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order as calculated at checkout. |
| checkoutTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts, as calculated at checkout. |
| checkoutTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to the order after checkout, including items, promotion codes, charges, and promotions. |
| checkoutTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order as calculated at checkout. |
| pickTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the time of picking. Excludes discounts and delivery or other charges. This is only displayed after the order has been picked. |
| pickTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the time of picking, including any discounts. Excludes delivery or other charges. This is only displayed after the order has been picked. |
| pickTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the time of picking. Excludes discounts. This is only displayed after the order has been picked. |
| pickTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the time of picking. Includes discounts. This is only displayed after the order has been picked. |
| pickTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order as calculated at the time of picking. |
| pickTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts, as calculated at the time of picking. This is only displayed after the order has been picked. |
| pickTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the order after picking. This is only displayed after the order has been picked. |
| pickTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order as calculated at the time of picking. |
| finalTotalItemsUndiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the moment of delivery. Excludes discounts and delivery or other charges. This is only displayed after the order has been delivered. |
| finalTotalItemsDiscountedAmount | STRING | From Data Dictionary: The total cost of goods in the order as calculated at the moment of delivery, including any discounts. Excludes delivery or other charges. This is only displayed after the order has been delivered. |
| finalTotalChargesUndiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery, as calculated at the moment of delivery. Excludes discounts. This is only displayed after the order has been delivered. |
| finalTotalChargesDiscountedAmount | STRING | From Data Dictionary: The total cost of all extra charges, including delivery and any discounts on charges. This is only displayed after the order has been delivered. |
| finalTotalAdditionalProductChargesAmount | STRING | From Data Dictionary: The total cost of additional product charges in the order at the time of delivery. |
| finalTotalReturnsTotalAmount | RECORD | From Data Dictionary: The total gain to the customer for all returns. This is only displayed after the order has been delivered. |
| finalTotalReturnsTotalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the tax applied to the buybacks amount, including boughtback bags. This is only displayed after the order has been delivered. |
| finalTotalFinalAmount | RECORD | From Data Dictionary: The total cost to the customer for all goods and extra charges, including any discounts. This is only displayed after the order has been delivered. |
| finalTotalFinalTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the final order amount. This is only displayed after the order has been delivered. |
| finalTotalDepositAmount | STRING | From Data Dictionary: The total cost of deposits in the order. |
| paymentIds | REPEATED STRING | From Data Dictionary: A list of unique identifiers for the payments associated with the order. |
| pricingContextId | STRING | \- |
| smallOrderChargeSellerId | STRING | From Data Dictionary: The ID for the seller associated with the small order charge. |
| smallOrderChargeUndiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for the small order charge, excluding any discounts. |
| smallOrderChargeDiscountedAmount | STRING | From Data Dictionary: The amount charged to the customer for the small order charge, including any discounts. |
| smallOrderChargeTaxedAmount | RECORD | From Data Dictionary: The sum of all taxes applied to the small order charge. |
| net | STRING | From Data Dictionary: The net amount. |
| gross | STRING | From Data Dictionary: The gross amount. |
| smallOrderChargePromotions | REPEATED RECORD | From Data Dictionary: Information about promotions applied to the small order charge. |
| retailerPromotionId | STRING | From Data Dictionary: The external ID for the small order charge promotion. |
| retailerPromotionCode | STRING | From Data Dictionary: The code used by the customer to trigger the small order charge promotion. |
| discountAmount | STRING | From Data Dictionary: The monetary value of the discount applied to the small order charge. |
| smallOrderChargeTaxComponents | REPEATED RECORD | From Data Dictionary: Information about the taxes applied to the small order charge. |
| taxCode | STRING | From Data Dictionary: The code of tax associated with the small order charge. Enumeration: A, B, etc., Z |
| taxRate | STRING | From Data Dictionary: The rate of tax applied to the small order charge amount. |
| taxableAmount | STRING | From Data Dictionary: The portion of the small order charge that is taxable. |
| taxAmount | STRING | From Data Dictionary: The amount of tax associated with the small order charge. |
| orderLoyaltySpentTaxComponents | REPEATED RECORD | \- |
| taxCode | STRING | \- |
| taxRate | STRING | \- |
| taxableAmount | STRING | \- |
| taxAmount | STRING | \- |
| fulfillmentCancelType | STRING | \- |
| fulfillmentCancelReason | STRING | From Data Dictionary: The reason why the fulfillment associated with the order was canceled. |

