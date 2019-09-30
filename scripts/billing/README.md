# Billing Analytics - BigQuery SQL Examples

In addition to the Billing product available within the GCP Console, you may want to export billing data to BigQuery for custom analytics.  Below are some example queries to help get you started using the standard data export format.  See the [links below](#useful-links) to learn more about GCP billing datasets including how to export your own billing data for analysis.

The example SQL snippets below reference a sample billing export available as a public dataset in BigQuery (**`bqutil.billing.billing_dashboard_export`**).  You will need to replace references to this table with your own project and dataset. 

## Example Queries

|Description|Time Period|Additional Dimensions|SQL|Additional Docs|
|:-|:-:|:-|:-:|:-:|
|Monthly total costs and credits|monthly|-|[Link](sql/monthly_costs_credits.sql)|-|
|Monthly invoice total costs and credits|monthly|-|[Link](sql/monthly_invoice_costs_credits.sql)|-|
|Monthly total costs by project|monthly|project|[Link](sql/monthly_costs_by_project.sql)|-|
|Previous month's net-charges per service|monthly|service|[Link](sql/previous_month_costs_by_service.sql)|-|
|Previous month’s net-charges by resource label|monthly|label|[Link](sql/previous_month_costs_by_label.sql)|-|
|Previous month’s per-service charges by label value|monthly|service,label|[Link](sql/previous_month_costs_by_service_label.sql)|-|
|Monthly CUD/SUD savings|monthly|-|[Link](sql/monthly_cud_sud_savings.sql)|[Link](docs/monthly_cud_sud_savings.md)|
|Daily compute usage CUD/SUD coverage|daily|region, unit, project, sku|[Link](sql/daily_compute_usage_cud_sud_coverage.sql)|[Link](docs/daily_compute_usage_cud_sud_coverage.md)|
|Previous day's spend per project|daily|project|[Link](sql/previous_day_costs_by_project.sql)|-|
|Previous day's spend per GCP Service|daily|service|[Link](sql/previous_day_costs_by_service.sql)|-|
|Daily compute usage hours|daily|sku, component|[Link](sql/daily_compute_usage_hours.sql)|-|
|Daily compute cost including discount|daily|-|[Link](sql/daily_compute_discount.sql)|-|
|Daily average number of cores executed|daily|-|[Link](sql/daily_compute_cores_average.sql)|-|
|Total costs by label value|total|label|[Link](sql/total_costs_by_label_value.sql)|-|
|Total costs by SKU and labels|total|sku, label|[Link](sql/total_costs_by_sku_label.sql)|-|

<a id='useful-links'></a>
## Useful Links

- [Cloud Billing Documentation](https://cloud.google.com/billing/docs/)
- [Export Billing Data to BigQuery](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- [Billing Export Contents](https://cloud.google.com/billing/docs/how-to/export-data-file#contents_of_the_exported_billing_file)
