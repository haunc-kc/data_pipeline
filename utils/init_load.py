def initial_load(df, table_name, cluster_cols):
    """Write initial load with Liquid Clustering enabled."""
    print(f"[INFO] Table {table_name} not found writing initial load with clustering ...")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy(*cluster_cols)
        .saveAsTable(table_name)
    )
    print(f"[DONE] Initial load written to {table_name}.")
