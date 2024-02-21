import io

from sklearn.cluster import KMeans
import snowflake.snowpark.types as T
import joblib
  

def load_model(p_session, snowflake_stage_name, file_name):
    p_session.file.get(f"@{snowflake_stage_name}/{file_name}", "/tmp")
    return joblib.load(os.path.join("/tmp", file_name))


def save_model(session, model, snowflake_stage_name, file_name):
    input_stream = io.BytesIO()
    joblib.dump(model, input_stream)
    session._conn.upload_stream(input_stream, snowflake_stage_name, file_name)


def model(dbt, session):
    dbt.config(
        packages=['scikit-learn', 'pandas', 'numpy'],
    )

    # create a stage in Snowflake to save our model file
    session.sql('create stage if not exists MODELSTAGE').collect()

    # guess at the number of clusters
    n_clusters = 5

    # get upstream data
    orders_with_subtotals = dbt.ref("orders_with_subtotals").to_pandas()

    try:
        # try to load existing model from Snowflake stage
        model = load_model(session, "@MODELSTAGE", "orders_cluster_v1.joblib")
    except:
        # drop non-numeric columns
        X = orders_with_subtotals.select_dtypes(
            include=["float32", "float64", "int32", "int64"]
        ).to_numpy()

        # train model and save it to Snowflake stage
        kmeans = KMeans(n_clusters=n_clusters)
        model = kmeans.fit(X)
        save_model(session, model, "@MODELSTAGE", "orders_cluster_v1.joblib")

    # score model
    cluster_labels = model.predict(X)

    # add cluster labels to orders_with_subtotals
    temp = session.create_dataframe(
        data=cluster_labels.tolist(),
        schema=T.StructType([T.StructField("cluster_label", T.IntegerType())]),
    ).to_pandas()

    orders_with_subtotals_and_clusters = orders_with_subtotals.merge(
        temp, left_index=True, right_index=True
    )

    return session.create_dataframe(orders_with_subtotals_and_clusters)
