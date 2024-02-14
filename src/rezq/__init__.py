import datetime
import logging

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, lit

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def add_constraint(spark_session: SparkSession, delta_table: DeltaTable, constraint_name: str, constraint_code: str):
    """
    Add Constraint: Add constraint to a Delta Table.

    Args:
        spark_session (SparkSession): A SparkSession.
        delta_table (DeltaTable): The Delta Table to be analyzed.
        constraint_name (str): The constraint name to be added.
        constraint_code (str): The constraint code to be added.
    """
    location = delta_table.detail().select("location").take(1)[0][0]
    query = f"""
        ALTER TABLE delta.`{location}`
        ADD CONSTRAINT {constraint_name} CHECK ({constraint_code})
    """
    spark_session.sql(query)
    logger.info(f'Constraint {constraint_name} successfully added')


def add_constraints(spark_session: SparkSession, delta_table: DeltaTable, constraints: dict):
    """
    Add Constraints: Add a list of constraint to a Delta Table.

    Args:
        spark_session (SparkSession): A SparkSession.
        delta_table (DeltaTable): The Delta Table to be analyzed.
        constraints (dict): The constraints, with name as key and code as value to be added.
    """
    location = delta_table.detail().select("location").take(1)[0][0]
    for constraint_name, constraint_code in constraints.items():
        query = f"""
            ALTER TABLE delta.`{location}`
            ADD CONSTRAINT {constraint_name} CHECK ({constraint_code})
        """
        spark_session.sql(query)
        logger.info(f'Constraint {constraint_name} successfully added')


def remove_constraint(spark_session: SparkSession, delta_table: DeltaTable, constraint_name: str):
    """
    Remove Constraints: Remove constraint of a Delta Table.

    Args:
        spark_session (SparkSession): A SparkSession.
        delta_table (DeltaTable): The Delta Table to be analyzed.
        constraint_name (str): The constraint name to be removed.
    """
    location = delta_table.detail().select("location").take(1)[0][0]
    query = f"""
        ALTER TABLE delta.`{location}`
        DROP CONSTRAINT {constraint_name}
    """
    spark_session.sql(query)
    logger.info(f'Constraint {constraint_name} successfully removed')


def get_constraints(delta_table: DeltaTable):
    """
    Get Constraints: Return constraints of a Delta Table.

    Args:
        delta_table (DeltaTable): The Delta Table to be analyzed.
    Returns:
        constraints: An dictionary of constraints.
    """
    properties = delta_table.detail().select("properties").collect()[0]["properties"]
    constraints = {
        k: v for k, v in properties.items() if k.startswith("delta.constraints")
    }
    if constraints:
        return constraints
    else:
        logger.info("No constraints found in the Delta table.")
        return None


def get_cleaned_and_quarantine(delta_table: DeltaTable, merge_df: DataFrame, add_not_null=True):
    """
    Get Cleaned and Quarantine: Return a cleaned and quarantine DataFrame.

    Args:
        delta_table (DeltaTable): The Delta Table for which to create a graphic.
        merge_df (DataFrame): The DataFrame to be analyzed.
        add_not_null (bool): The parameter to add a default not null constraint to all fields in the DataFrame.

    Returns:
        cleaned_df: An DataFrame of the valid rows according to constraints.
        quarantine_df: An DataFrame of the invalid rows according to constraints.
    """

    if not isinstance(delta_table, DeltaTable) and not type(delta_table).__name__ == 'DeltaTable':
        raise TypeError("An existing delta table must be specified for delta_table.")

    if not isinstance(merge_df, DataFrame) and not type(merge_df).__name__ == 'DataFrame':
        raise TypeError("You must provide a DataFrame.")

    properties = delta_table.detail().select("properties").collect()[0]["properties"]
    constraints = {
        k: v for k, v in properties.items() if k.startswith("delta.constraints")

    }

    if add_not_null:
        fields = delta_table.toDF().schema.fields
        null_constraints = {
            f'{field.name}_is_not_null': f"{field.name} is not null" for field in fields
        }
        constraints.update(null_constraints)

    if not constraints:
        raise TypeError("There are no constraints present in the target delta table")

    quarantine_df = merge_df.filter(
        "not (" + " and ".join([c for c in constraints.values()]) + ")"
    )
    when_cases = [f"WHEN (not {v}) THEN '{k.split('.')[-1]}'" for k, v in constraints.items()]
    rules_expr = "CASE " + " ".join(when_cases) + " ELSE 'unkown' END"
    quarantine_df = quarantine_df.withColumn('violated_constraint', expr(rules_expr)) \
        .withColumn('created_at', lit(datetime.datetime.utcnow()))

    cleaned_df = merge_df.filter(" and ".join([c for c in constraints.values()]))

    return cleaned_df, quarantine_df
