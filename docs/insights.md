# RetailCo Business Insights
## Weekly Management Report

---

## 1. Revenue Performance

**Which stores, products, and categories are driving sales, and how does it trend over time?**

RetailCo generated strong revenue across all four store locations, with relatively balanced performance across the network. Lagos leads with **₦16.3 billion** in total revenue, followed closely by Kano (**₦16.2 billion**), Abuja (**₦16.2 billion**), and Port Harcourt (**₦16.0 billion**). The near-equal distribution across stores suggests consistent operational performance and demand across all four cities, with no single store significantly outperforming others.

| Store | Total Revenue |
|-------|-------------|
| RetailCo Lagos | ₦16,305,522,090 |
| RetailCo Kano | ₦16,244,918,535 |
| RetailCo Abuja | ₦16,243,442,673 |
| RetailCo Port Harcourt | ₦15,999,053,029 |

From a category perspective, **Sports** is the top revenue driver at ₦8.87 billion, followed by **Home & Garden** (₦8.75 billion) and **Books & Media** (₦8.72 billion). These three categories are remarkably close in performance, suggesting broad, diversified demand rather than concentration in a single category.

Monthly revenue trends show **August 2025** as the strongest month (₦2.81 billion), followed by **March 2025** (₦2.78 billion) and **March 2026** (₦2.78 billion). The consistency of March across two consecutive years suggests a seasonal pattern, possibly driven by post-New Year spending or school resumption cycles common in Nigeria.

**Recommendation:** Invest in inventory and marketing for August and March, as these months consistently deliver peak revenue.

---

## 2. Customer Behaviour

**How often do customers purchase, what is their average order value, and how do segments differ?**

RetailCo serves four distinct customer segments, which are; retail, wholesale, VIP, and corporate, with relatively balanced distribution across all four.

| Segment | Customer Count | Avg Order Value |
|---------|---------------|----------------|
| Corporate | 1,233 | ₦189,419 |
| VIP | 1,242 | ₦189,400 |
| Retail | 1,272 | ₦188,697 |
| Wholesale | 1,253 | ₦188,480 |

Notably, average order values are nearly identical across all segments, ranging from ₦188,480 to ₦189,419, which is a difference of less than 0.5%. This suggests that RetailCo's pricing and product mix does not meaningfully differentiate between segment types, which may be an opportunity to develop segment-specific offers.

Corporate and VIP customers show marginally higher average order values, which aligns with expectations, as these customers typically purchase higher-value items or in larger quantities. However, the retail segment has the highest customer count (1,272), making it the broadest base for growth.

**Recommendation:** Develop loyalty programmes targeting the retail segment to increase purchase frequency, and create premium bundles for VIP and corporate customers to widen the average order value gap.

---

## 3. Product & Discount Analysis

**What sells, what gets discounted, and what is the margin impact?**

Sports, Home & Garden, and Books & Media are the three leading categories by revenue, each contributing approximately ₦8.7-8.9 billion. The tight revenue clustering across categories suggests a well-balanced product mix without over-reliance on any single category.

The pipeline tracks discount amounts at the order line level via `fct_sales.discount_amount`, enabling margin impact analysis. Products with high discount percentages but low net revenue contribution can be identified and reviewed for pricing strategy adjustments.

The presence of **342,830 order line items** across **80,000 orders** gives an average of approximately **4.3 line items per order**, indicating customers regularly purchase multiple products per transaction - a positive signal for cross-selling opportunities.

**Recommendation:** Review discount policies for high-volume, low-margin products. Focus cross-selling efforts on the Sports category, which leads all categories in revenue.

---

## 4. Payment Channel Insights

**Which payment methods are used, and are there anomalies?**

All five payment methods show remarkably balanced usage, both in transaction count and total amount processed.

| Payment Method | Transactions | Total Amount |
|---------------|-------------|-------------|
| USSD | 13,931 | ₦11,962,532,220 |
| Mobile Money | 13,853 | ₦11,897,113,734 |
| Cash | 13,845 | ₦11,768,451,173 |
| Card | 13,873 | ₦11,714,108,280 |
| Bank Transfer | 13,700 | ₦11,620,617,369 |

**USSD** leads in both transaction volume and total amount, reflecting Nigeria's widespread adoption of USSD-based banking, particularly among customers who may not have smartphones or reliable internet access. **Mobile Money** follows closely, consistent with the growing fintech adoption across Nigeria.

The near-equal distribution across all five channels is a positive sign which indicates that RetailCo is not over-dependent on any single payment method, reducing operational risk.

**Anomalies:** The pipeline flagged **2,858 anomalous payments** isolated into the `flagged_payments` table:
- **2,166** unexplained negative amounts - payments recorded as negative without a corresponding refund status
- **692** zero-amount payments - transactions with no value recorded

These anomalies represent approximately **4% of all payment records** and have been excluded from revenue calculations. They warrant investigation with the finance team to determine whether they are system errors, cancelled transactions, or data entry issues.

**Recommendation:** Investigate the 2,166 unexplained negative payments immediately. Implement validation rules in the ERP system to prevent zero-amount payment records from being created.

---

## 5. Operational Data Quality

**What anomalies exist in the raw data, and how are they flagged?**

The pipeline implements comprehensive data quality handling at multiple layers:

| Issue | Count | Handling |
|-------|-------|---------|
| Anomalous payments (zero amount) | 692 | Isolated to `flagged_payments` |
| Unexplained negative payments | 2,166 | Isolated to `flagged_payments` |
| Soft-deleted records | Present | Preserved with `is_deleted` flag |
| Late-arriving order updates | Ongoing | Handled via idempotent upserts |
| SCD2 customer/product changes | Tracked | Full history in snapshots |

The pipeline successfully handles all known data quality issues documented in the ERP API specification. The `flagged_payments` table serves as a quarantine zone, ensuring anomalous records are visible and auditable without contaminating revenue metrics.

The extractor also handles transient API errors (approximately 3% of requests return 500 errors) through exponential backoff with up to 5 retries, and respects rate limiting via the `Retry-After` header on 429 responses.

**Recommendation:** Schedule a monthly data quality review using the `flagged_payments` table as the primary input. Work with the ERP vendor to fix the root causes of zero-amount and unexplained negative payment records.

---

*This report was generated from the RetailCo data warehouse. Data covers May 2024 to May 2026 across all four store locations.*