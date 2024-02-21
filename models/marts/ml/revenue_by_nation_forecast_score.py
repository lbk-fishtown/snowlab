import pandas as pd
from prophet import Prophet
from prophet.serialize import model_from_json


def model(dbt, session):
    dbt.config(
        packages=["prophet","holidays==0.18", "pandas"],
    )

    # get trained ML models
    models = dbt.ref("revenue_by_nation_forecast_train").to_pandas()

    # get most recent trained_at
    most_recent_trained_at = models["TRAINED_AT"].max()

    # filter models by most recent trained_at
    models = models[models["TRAINED_AT"] == most_recent_trained_at]

    # get list of unique nations dynamically
    nations = sorted(list(models["NATION"].unique()))

    # hydrate models as Prophet objects
    models = {
        nation: model_from_json(
            models[models["NATION"] == nation]["MODEL"].iloc[0]
        )
        for nation in nations
    }

    # create future dataframe to forecast on
    future = models[nations[0]].make_future_dataframe(periods=52 * 3, freq="W")

    # score model per nation
    forecasts = {
        nation: models[nation].predict(future)
        for nation in nations
    }

    # dataframe magic (use nation to filter forecasts from single table)
    for nation, forecast in forecasts.items():
        forecast["nation"] = nation

    return session.create_dataframe(pd.concat(forecasts.values()))
