#example from https://towardsdatascience.com/reduce-warehouse-space-with-the-pareto-principle-using-python-e722a6babe0e
#optimizing warehouse location based on sku frequency using pareto 80/20 principle 

import pandas as pd


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["pandas"] #https://repo.anaconda.com/pkgs/snowflake/
    )

    df_parts = dbt.ref("dim_parts").to_pandas()
    df_orders = dbt.ref("fct_order_items").to_pandas()
    
    df_merged = df_orders.merge(df_parts, on='PART_KEY')

    # Quantity/sku
    df_par = pd.DataFrame(df_merged.groupby(['PART_KEY'])['QUANTITY'].sum())
    df_par.columns = ['QUANTITY']

    # Sort Values
    df_par.sort_values(['QUANTITY'], ascending = False, inplace = True)
    df_par.reset_index(inplace = True)

    # Cumulative Sum 
    df_par['CumSum'] = df_par['QUANTITY'].cumsum()

    # % CumSum
    df_par['PERCENT_CumSum'] = (100 * df_par['CumSum']/df_par['QUANTITY'].sum())

    # % SKU
    df_par['PERCENT_SKU'] = (100 * (df_par.index + 1).astype(float)/(df_par.index.max() + 1))

    df_par.set_index('PART_KEY').head()

    return df_par

