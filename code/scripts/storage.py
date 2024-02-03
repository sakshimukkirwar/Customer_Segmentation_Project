from sqlalchemy import create_engine



account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"



snowflake_options = {
    "sfURL": "https://fx34478.us-central1.gcp.snowflakecomputing.com",
    "sfUser": "DATA228PROJECT",
    "sfPassword": "Project228",
    "sfWarehouse": "COMPUTE_WH",
    "sfDatabase": "data_228_project",
    "sfSchema": "yelp",
    "sfTable": "test",
    "dbtable": "test"
}
#This dictionary stores Snowflake connection details and table information for the specified Snowflake account, user, password, warehouse, database, schema, and table, as well as the corresponding database table used in Spark.

def get_engine():
    engine = create_engine(
        'snowflake://{user}:{password}@{account}'.format(user=snowflake_options["sfUser"],
                                                         password=snowflake_options["sfPassword"],
                                                         account=account)
    )
    return engine

#This function saves a Spark DataFrame to a Snowflake database table, updating the Snowflake connection options with the specified table name and then using the Snowflake DataFrame writer to overwrite the specified table.
def save_spark_df_to_db(df, table):
    current_conf = snowflake_options
    current_conf['dbtable'] = table
    current_conf['sfTable'] = table
    (
        df.write.format("snowflake")
        .options(**current_conf)
        .mode("overwrite")
        .save(table)
    )


