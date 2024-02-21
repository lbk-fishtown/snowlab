def model(dbt, session):
    # get upstream data
    orders = dbt.ref("fct_orders").to_pandas()
    parts = dbt.ref("dim_parts").to_pandas()
    order_items = dbt.ref("fct_order_items").to_pandas()

    # list of unique product types
    product_types = sorted(list(set(parts["TYPE"].unique())))

    # get the subtotal for each product type for each order_key
    order_item_product_subtotals = (
        order_items.merge(parts, on="PART_KEY")
        .groupby(["ORDER_KEY", "TYPE"])
        .agg(subtotal=("RETAIL_PRICE", "sum"))
        .reset_index()
        .pivot(index="ORDER_KEY", columns="TYPE", values="subtotal")
        .reset_index()
    )

    # rename the type columns to include "subtotal_"
    order_item_product_subtotals.rename(
        columns={
            product_type: f"subtotal_{product_type.replace(' ', '_')}"
            for product_type in product_types
        },
        inplace=True,
    )

    # fill NaNs with 0s
    order_item_product_subtotals = order_item_product_subtotals.fillna(0)

    # merge with the existing orders mart
    orders_with_subtotals = orders.merge(order_item_product_subtotals, on="ORDER_KEY")

    return session.create_dataframe(orders_with_subtotals)

    # return orders
