## Example Output
The resulting table will include costs from all GCE SKUs. SKUs to which CUD or SUD have been applied will show both the credit $ amount and the usage amount offset by the credit. This is the schema of the output table, along with an example value.

| **Field Name** | **Description** | **Example Value** |
| :- | :- | :- |
| usage_date | Date (Los Angeles Time) in which the usage occurred | 2019-07-23 |
| region | Location.region in Billing Export schema | us-central1 |
| unit | Usage.unit in Billing Export schema | seconds |
| project_id | Project.id in Billing Export schema | committed-use-discount-test2 |
| project_name | project.name in Billing Export schema | committed-use-discount-test2 |
| sku_id | Sku.id in Billing Export schema | 2E27-4F75-95CD |
| sku_description | Sku.description in Billing Export system labels | N1 Predefined Instance Core running in Americas |
| machine_spec | See Billing Export system labels | n1-standard-1 |
| usage_amount | Usage.amount in Billing Export schema  | 184590.0 |
| cost | See Billing Export schema | 1.620846 |
| CUD_covered_usage | Amount of usage covered by CUD; unit is indicated by the unit field. | 147668.06959875411 |
| CUD_cost | $ amount offset by CUD credits; in the currency of your Billing Account, as indicated by the currency field of your billing export  | -1.296641 |
| SUD_covered_usage | Amount of usage covered by SUD; unit is indicated by the unit field. | 14768.276077462948 |
| SUD_cost | $ amount offset by SUD credits; in the currency of your Billing Account, as indicated by the currency field of your billing export  | -0.129677 |