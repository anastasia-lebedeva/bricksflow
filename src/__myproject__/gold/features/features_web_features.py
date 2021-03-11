# Databricks notebook source
# MAGIC %md
# MAGIC #### Overview
# MAGIC ##### Features:
# MAGIC 
# MAGIC * web_analytics_mobile_user
# MAGIC * web_analytics_time_on_site_avg
# MAGIC * web_analytics_loan_visits_count
# MAGIC * web_analytics_mortgage_visits_count
# MAGIC * web_analytics_loan_last_visit_date
# MAGIC * web_analytics_loan_days_since_last_visit
# MAGIC * web_analytics_mortgage_last_visit_date
# MAGIC * web_analytics_mortgage_days_since_last_visit
# MAGIC * web_analytics_device_type_most_common
# MAGIC * web_analytics_device_type_last_used
# MAGIC * web_analytics_totals_visits
# MAGIC * web_analytics_blog_last_visit_date
# MAGIC * web_analytics_last_visit_date
# MAGIC * web_analytics_pageviews_sum
# MAGIC * web_analytics_channel_group_most_common
# MAGIC * web_analytics_events_sum

# COMMAND ----------

# MAGIC %md #### Init

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------

# MAGIC %md #### Imports

# COMMAND ----------


from pyspark.sql.window import Window
from datetime import datetime, timedelta
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from pyspark.sql import SparkSession
from __myproject__.gold.features.feature import feature, featureLoader, clientFeature
from __myproject__.gold.features.feature import FeatureStore
from pyspark.sql import types as t
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f


# COMMAND ----------

# MAGIC %md #### Input

# COMMAND ----------

@dataFrameLoader(display=False)
def read_sdm_web_data(spark: SparkSession):
    return spark.read.table('sdm.web_data')


@dataFrameLoader(display=False)
def read_web_data_detail(spark: SparkSession):
    return spark.read.table('sdm.web_data_detail')

# COMMAND ----------

# df_web_data = spark.read.parquet(
#     "abfss://csas@dslabdigilakegen2.dfs.core.windows.net/DATA/EXPORT_PERSONA360/web_data_tables/web_data_sample_20200901_20201231"
# )

# df_web_data_detail = spark.read.parquet(
#     "abfss://csas@dslabdigilakegen2.dfs.core.windows.net/DATA/EXPORT_PERSONA360/web_data_tables/web_data_detail_sample_20200901_20201231"
# )

# COMMAND ----------

# MAGIC %md ##### Input parameters - Widgets
# MAGIC * run_date 
# MAGIC * time_window

# COMMAND ----------


#dbutils.widgets.text('time_window', '90', 'time_window')
#dbutils.widgets.text('run_date', ((datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")), 'run_date')

time_window = 90
run_date = datetime.now().date().strftime("%Y%m%d")
start_date = (datetime.strptime(str(run_date), "%Y%m%d") - timedelta(days=time_window)).strftime("%Y%m%d")

print(f'Using start_date: {start_date}')
print(f'Using run_date: {run_date}')

# COMMAND ----------

# MAGIC %md #### Constants

# COMMAND ----------

prefix_name = 'web_analytics'
suffix_name = '__a0'

# COMMAND ----------

# MAGIC %md #### Register feature (or compute value for a `run_date`)

# COMMAND ----------

# New: add tuntimecolumn
@transformation(read_sdm_web_data, display=False)
def sdm_web_data_with_rundate_filtered(df: DataFrame):
    return (df
           .withColumn('run_date', f.lit(run_date))
           .select(
                "session_start_datetime",
                "date",
                "client_id_hash",
                "device_type",
                "run_date"
        )
        .filter(f.col("date") >= start_date)
        .filter(f.col("date") <= int(run_date))
    )

# COMMAND ----------
# feature: web_analytics_mobile_user 


@clientFeature(
    sdm_web_data_with_rundate_filtered,
    feature_name=[f"{prefix_name}_mobile_user_{time_window}days", f"{prefix_name}_tablet_user_{time_window}days"],
    description=["Web analytics feature for mobile user", "Web analytics feature for tablet user"],
    dtype=["DOUBLE", "DOUBLE"],
    timeid_column="run_date",
    skip_computed=True,
    write=True,
    display=True
)
def feature_mobile_tablet_user_for_tw(df: DataFrame):

    feature_mob_name=f"{prefix_name}_mobile_user_{time_window}days"
    feature_tabl_name=f"{prefix_name}_tablet_user_{time_window}days"

    # aggregation/calculation of feature
    return (
        df.drop_duplicates()
        .withColumn(
            "is_mobile",
            f.when((f.col("device_type") == "mobile"), 1)
            .when((f.col("device_type") == ("tablet"))
                    | (f.col("device_type") == ("desktop")), 0
                )
        )
        .withColumn(
            "is_tablet",
            f.when((f.col("device_type") == "tablet"), 1).when(
                (f.col("device_type") == ("mobile"))
                | (f.col("device_type") == ("desktop")), 0
            )
        )
        .groupBy("client_id_hash", "run_date")
        .agg(
            f.round(f.avg("is_mobile"), 1).alias(feature_mob_name),
            f.round(f.avg("is_tablet"), 1).alias(feature_tabl_name)
        )
    )

# COMMAND ----------

@clientFeature(
    sdm_web_data_with_rundate_filtered,
    feature_name=f"{prefix_name}_desktop_user_{time_window}days",
    description="Web analytics feature for desktop user",
    dtype="DOUBLE",
    timeid_column="run_date",
    skip_computed=True,
    write=True,
    display=True
)
def feature_desktop_user_for_tw(df: DataFrame):

    feature_desc_name=f"{prefix_name}_desktop_user_{time_window}days"

    # aggregation/calculation of feature
    return (
        df.drop_duplicates()
        .withColumn(
            "is_desktop",
            f.when((f.col("device_type") == "desktop"), 1).when(
                (f.col("device_type") == ("mobile"))
                | (f.col("device_type") == ("tablet")),
                0
            )
        )
        .groupBy("client_id_hash", "run_date")
        .agg(
            f.round(f.avg("is_desktop"), 1).alias(feature_desc_name)
        )
    )

# COMMAND ----------

# MAGIC %md #### Access values in feature store

# @featureLoader(display=True)
# def load_features_onefeature(feature_store: FeatureStore):
#     return feature_store.get(entity_name='client',
#                             feature_name_list=['web_analytics_desktop_user_90days'])

# @featureLoader(display=True)
# def load_features_multiple(feature_store: FeatureStore):
#     return feature_store.get(entity_name='client',
#                             feature_name_list=['web_analytics_desktop_user_90days', 'web_analytics_desktop_user_120days'])
                        
# @featureLoader(display=True)
# def load_features_all(feature_store: FeatureStore):
#     return feature_store.get(entity_name='client')

# @featureLoader(
#     sdm_web_data_with_rundate_filtered,
#     display=True
# )
# def load_features_for_id_timeid(df: DataFrame, feature_store: FeatureStore):
#      return feature_store.get_for_id_timeid(
#         df_id_timeid=df,
#         entity_name='client',
#         feature_name_list=['web_analytics_desktop_user_90days'],
#         df_id_column_name='client_id_hash',
#         df_timeid_column_name='run_date'
# )