# rezQ

## Descrição

rezQ is a Python library designed to help ensure data quality in Delta Lake. It provides functions to add constraints to data, remove existing constraints, and separate a DataFrame based on those constraints.

## Instalação
You can install rezQ using pip:

```bash
pip install rezq

```
## Add constraint
To add a constraint to a delta table:
```bash
from rezq import add_constraint
from delta import DeltaTable

delta_table = DeltaTable.forPath(spark, 'my_table_path')
add_constraint(spark_session=spark,
                delta_table=delta_table,
                constraint_name='age_higher_than_20',
                constraint_code='age > 20')
```
## Remove constraint
To remove a constraint to a delta table:
```bash
from rezq import, remove_constraint
from delta import DeltaTable

delta_table = DeltaTable.forPath(spark, 'my_table_path')
remove_constraint(spark_session=spark,
                delta_table=delta_table,
                constraint_name='age_higher_than_20')
```
## Get constraints
To get delta table constraints:
```bash
from rezq import, remove_constraint
from delta import DeltaTable

delta_table = DeltaTable.forPath(spark, 'my_table_path')
constraint = get_constraints(delta_table)
```

## Get cleaned and quarantine
To return the cleaned_df (An DataFrame of the valid rows according to constraints)
and quarantine_df (An DataFrame of the invalid rows according to constraints.):
```bash
from rezq import get_cleaned_and_quarantine

delta_table = DeltaTable.forPath(spark, 'mybucket')

cleaned_df, quarantine_df = get_cleaned_and_quarantine(delta_table=delta_table,
                                        merge_df=df)

```