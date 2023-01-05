import sys
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def main(session: Session, database_name: str) -> str:
    # Set the context
    # You can only use USE statements in a sproc if it's created using Caller's Rights
#    session.use_warehouse("DEMO_WH")
#    session.use_database("DEMO_DB")
#    session.use_schema("TKO")
    table_name_fq = f"{database_name}.HARMONIZED.DIM_CUSTOMER"

    # Define the schema for the dim_customer table
    dim_customer_schema = T.StructType([T.StructField("CUSTOMER_ID", T.IntegerType()),
                                    T.StructField("FIRST_NAME", T.StringType()),
                                    T.StructField("LAST_NAME", T.StringType()),
                                    T.StructField("CITY", T.StringType()),
                                    T.StructField("COUNTRY", T.StringType()),
                                    T.StructField("POSTAL_CODE", T.StringType()),
                                    T.StructField("PREFERRED_LANGUAGE", T.StringType()),
                                    T.StructField("GENDER", T.StringType()),
                                    T.StructField("FAVOURITE_BRAND", T.StringType()),
                                    T.StructField("MARITAL_STATUS", T.StringType()),
                                    T.StructField("CHILDREN_COUNT", T.StringType()),
                                    T.StructField("SIGN_UP_DATE", T.DateType()),
                                    T.StructField("BIRTHDAY_DATE", T.DateType()),
                                    T.StructField("E_MAIL", T.StringType()),
                                    T.StructField("PHONE_NUMBER", T.StringType()),
                                    T.StructField("TOTAL_SALES", T.DecimalType())
                        ])

    # Create the dim_customer table if it doesn't exist
    try:
        dim_customer = session.table(table_name_fq)
        _ = dim_customer.count()
    except:
        dim_customer = session.create_dataframe([[None]*len(dim_customer_schema.names)], schema=dim_customer_schema)\
                            .na.drop()\
                            .write.mode('overwrite').save_as_table(table_name_fq)
        dim_customer = session.table(table_name_fq)

    # Build out the stage dataframe
    dim_customer_stage = session.table(f"{database_name}.RAW_CUSTOMER.CUSTOMER_LOYALTY") \
                            .select(F.col("CUSTOMER_ID"), \
                                    F.col("FIRST_NAME"), \
                                    F.col("LAST_NAME"), \
                                    F.col("CITY"), \
                                    F.col("COUNTRY"), \
                                    F.col("POSTAL_CODE"), \
                                    F.col("PREFERRED_LANGUAGE"), \
                                    F.col("GENDER"), \
                                    F.col("FAVOURITE_BRAND"), \
                                    F.col("MARITAL_STATUS"), \
                                    F.col("CHILDREN_COUNT"), \
                                    F.col("SIGN_UP_DATE"), \
                                    F.col("BIRTHDAY_DATE"), \
                                    F.col("E_MAIL"), \
                                    F.col("PHONE_NUMBER")
                                )
    tot_sales = session.table(f"{database_name}.RAW_POS.ORDER_HEADER").group_by('CUSTOMER_ID') \
                        .sum(F.col("ORDER_TOTAL")) \
                        .with_column_renamed("CUSTOMER_ID", "CUSTOMER_ID_SALES") \
                        .with_column_renamed("SUM(ORDER_TOTAL)", "TOTAL_SALES")

    dim_customer_stage = dim_customer_stage.join(tot_sales, \
                                                dim_customer_stage['CUSTOMER_ID'] == tot_sales['CUSTOMER_ID_SALES'], \
                                                how = 'left') \
                                            .drop('CUSTOMER_ID_SALES') \

#    dim_customer.limit(100).show()

    # TODO: Need to show how to add a column via expression, or not set all columns like during an update
    # Merge (upsert) the stage dataframe into dim_customer
    # Dictionary argument to update() and insert() looks like this:
    # {c: dim_customer_stage[c] for c in dim_customer_schema.names}
    # which returns a dictionary that looks like this:
    # {'CUSTOMER_ID': Column("CUSTOMER_ID"), 'FIRST_NAME': Column("FIRST_NAME"), ...}
    dim_customer.merge(dim_customer_stage, dim_customer['CUSTOMER_ID'] == dim_customer_stage['CUSTOMER_ID'], \
                            [F.when_matched().update({c: dim_customer_stage[c] for c in dim_customer_schema.names}), F.when_not_matched().insert({c: dim_customer_stage[c] for c in dim_customer_schema.names})])

#    dim_customer.limit(100).show()

    return f"Successfully processed {table_name_fq}"


# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == '__main__':
    from local_connection import get_dev_config
    session = Session.builder.configs(get_dev_config('dev')).create()
    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore
    session.close()
