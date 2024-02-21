from datetime import datetime

from prophet import Prophet
from prophet.serialize import model_to_json
import snowflake.snowpark.types as T


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        packages=["prophet","holidays==0.18"],
    )

    # use current time as index
    trained_at = datetime.now()

    # get upstream data
    revenue = dbt.ref("revenue_by_nation").to_pandas()

    # rename to match prophet's expected column names
    renames = {
        "DATE_WEEK": "ds",
        "REVENUE": "y",
    }
    revenue = revenue.rename(columns=renames)

    # get list of unique nations dynamically
    nations = sorted(list(revenue["NATION"].unique()))

    # train the ML models per nation
    models = [
        Prophet().fit(revenue[revenue["NATION"] == nation])
        for nation in nations
    ]

    # persist models
    return session.create_dataframe(
        data=[
            (trained_at, nation, model_to_json(model))
            for nation, model in zip(nations, models)
        ],
        schema=T.StructType([
            T.StructField("trained_at", T.TimestampType()),
            T.StructField("nation", T.StringType()),
            T.StructField("model", T.StringType()),
        ]),
    )
