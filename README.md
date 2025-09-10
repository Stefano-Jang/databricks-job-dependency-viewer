# jiig

JIIG denotes "Job Incidents Identification Graph". <br>
A single Job may generate multiple tables that serve as inputs for other downstream Jobs. When a failure occurs, administrators must determine the downstream dependencies and notify affected teams. Currently, admin team identifies failed Jobs and manually traces their downstream sub-Jobs before notifying the respective owners via Slack. This manual failure-handling process typically takes more than hours, consuming significant time and resources. <br>
The goal of this project is to automate the detection and impact analysis of Job and Pipeline failures using system tables and table lineage information. The solution will also provide a visualization of dependencies—representing Jobs and Pipelines as nodes and their relationships as edges—through a Databricks App, enabling teams to quickly understand the scope of an incident.

The dashboard provides,
- Failed Job or Pipeline IDs, Name
- Affected Job or Pipeline IDs, Name, creator/run_as email, affected_tables <br>
_(affected_tables: Job A creates Table_1, Job B creates Table_2 based on Table_1, if Job A fails, the affected table is Table_1)_

- This project is DABs (**Databricks Asset Bundles**)
- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)

![Dashboard](resources/figures/jiig_dashboard.png)
![Tables](resources/figures/jiig_dashboard_table.png)

## Getting Started

To deploy and manage this asset bundle, follow these steps:

### Deployment
Download all sources, and import to your workspace

