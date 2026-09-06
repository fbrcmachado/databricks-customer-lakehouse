# Power BI connection

Phase 13 exposes the Gold BI star schema through the existing Databricks SQL
warehouse.

## Recommended mode for this project

Use **Import** for the current educational dataset. The dataset is small and
does not need query-time access to Databricks for every visual interaction.

DirectQuery remains a valid future option when freshness requirements justify
it.

## Connect from Power BI Desktop

1. Run:

```powershell
.\scripts\configure_sql_serving.ps1
```

2. Copy the printed `Server Hostname` and `HTTP Path`.

3. In Power BI Desktop select **Get data > Databricks**.

4. Enter the Server Hostname and HTTP Path.

5. Select **Import**.

6. For the current personal development environment select **OAuth** and sign
   in with the Databricks identity that already has access to the project.

7. In Navigator select:

```text
customer_lakehouse
└── gold_bi
    ├── dim_date
    ├── dim_customer
    ├── dim_product
    └── fact_sales
```

The views `v_sales_enriched` and `v_sales_monthly` are intentionally optional.
Use the dimensional tables for the semantic model.

## Relationships

Create these single-direction one-to-many relationships:

```text
dim_date[date_key]         1 ─── * fact_sales[date_key]
dim_customer[customer_key] 1 ─── * fact_sales[customer_key]
dim_product[product_key]   1 ─── * fact_sales[product_key]
```

Do not create relationships between dimensions.

## Initial measures

The repository includes `powerbi/measures.dax` with a small starter measure
set. Add the measures to the semantic model rather than creating calculated
columns for aggregations.

## Production authentication

The development choice is interactive OAuth. For a production Power BI
connection, replace personal authentication with a dedicated service principal
using machine-to-machine OAuth and grant only the required SQL warehouse and
Unity Catalog privileges.

## Decision expiration

Revisit Import mode when:

- freshness requirements become near-real-time;
- import refresh duration becomes operationally significant;
- dataset size approaches semantic-model capacity limits;
- row-level access must be enforced directly by Databricks at query time;
- consumers require query-time access to newly published Gold data.

At that point compare Import, DirectQuery, and any current Databricks/Power BI
integration capabilities rather than switching modes automatically.
