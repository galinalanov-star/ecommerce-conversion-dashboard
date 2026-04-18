SQL files for this project
## ▶️ How to Use

1. Open Google BigQuery
2. Use dataset: `bigquery-public-data.ga4_obfuscated_sample_ecommerce`
3. Run the query from `funnel_analysis.sql`
4. Export results for visualization in Tableau

## 🔄 Funnel Logic

The funnel includes the following steps:

- session_start
- view_item
- add_to_cart
- begin_checkout
- purchase

Each step is tracked using GA4 event_name.
