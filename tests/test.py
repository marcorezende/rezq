import datetime

from src.rezq import get_cleaned_and_quarantine, add_constraint, get_constraints, remove_constraint


def test_add_constraint(mock_delta_table, spark):
    add_constraint(spark_session=spark,
                   delta_table=mock_delta_table,
                   constraint_name='name_is_not_null',
                   constraint_code='name is not null')
    properties = mock_delta_table.detail().select("properties").collect()[0]["properties"]
    constraints = {
        k: v for k, v in properties.items() if k.startswith("delta.constraints")
    }
    assert constraints.get('delta.constraints.name_is_not_null') == 'name is not null'


def test_remove_constraint(mock_delta_table_with_name_constraint, spark):
    properties = mock_delta_table_with_name_constraint.detail().select("properties").collect()[0]["properties"]
    constraints = {
        k: v for k, v in properties.items() if k.startswith("delta.constraints")
    }
    assert constraints.get('delta.constraints.name_is_not_null') == 'name is not null'
    remove_constraint(spark_session=spark, delta_table=mock_delta_table_with_name_constraint,
                      constraint_name='name_is_not_null')
    properties = mock_delta_table_with_name_constraint.detail().select("properties").collect()[0]["properties"]
    constraints = {
        k: v for k, v in properties.items() if k.startswith("delta.constraints")
    }
    assert constraints.get('delta.constraints.name_is_not_null') is None


def test_get_constraints(mock_delta_table_with_age_constraint):
    constraints = get_constraints(delta_table=mock_delta_table_with_age_constraint)

    assert constraints.get('delta.constraints.age_higher_than_20') == 'age > 20'


def test_get_cleaned_and_quarantine(mock_delta_table_with_name_and_age_constraint, df_with_problems, spark):
    cdf, qdf = get_cleaned_and_quarantine(delta_table=mock_delta_table_with_name_and_age_constraint,
                                          merge_df=df_with_problems)
    cdf_collected = cdf.take(1)[0]
    qdf_collected = qdf.take(2)

    assert cdf_collected['name'] == 'Monica'
    assert cdf_collected['age'] == 30
    assert qdf_collected[0]['name'] is None
    assert qdf_collected[0]['age'] == 24
    assert qdf_collected[0]['violated_constraint'] == 'name_is_not_null'
    assert isinstance(qdf_collected[0]['created_at'], datetime.datetime)
    assert qdf_collected[1]['name'] == 'Franksuel'
    assert qdf_collected[1]['age'] == 15
    assert qdf_collected[1]['violated_constraint'] == 'age_higher_than_20'
    assert isinstance(qdf_collected[1]['created_at'], datetime.datetime)
