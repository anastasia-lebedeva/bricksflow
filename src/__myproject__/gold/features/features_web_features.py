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

# %run /dev/_init

# COMMAND ----------

# MAGIC %md #### Imports

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from datalakebundle.notebook.decorators import dataFrameLoader, transformation, dataFrameSaver, notebookFunction
from pyspark.sql import SparkSession
from __myproject__.gold.features.feature import feature, featureLoader
from __myproject__.gold.features.feature import FeatureStore
from pyspark.sql import types as t
from pyspark.sql.dataframe import DataFrame

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

run_params = {'time_window': 150,
                     'prefix_name': 'web_analytics',
                     'suffix_name': '__a0'}

time_window = run_params['time_window']
run_date = datetime.now().date().strftime("%Y%m%d")
start_date = (datetime.strptime(str(run_date), "%Y%m%d") - timedelta(days=time_window)).strftime("%Y%m%d")

print(f'Using start_date: {start_date}')
print(f'Using run_date: {run_date}')

# COMMAND ----------

# MAGIC %md #### Constants

# COMMAND ----------

# prefix_name = 'web_analytics'
# suffix_name = '__a0'

# COMMAND ----------

# MAGIC %md #### Transformations

# COMMAND ----------

# feature: web_analytics_mobile_user 

# New: add tuntimecolumn
from pyspark.sql.functions import lit

@transformation(read_sdm_web_data, display=False)
def sdm_web_data_with_rundate(df: DataFrame):
    return df.withColumn('run_date', lit(run_date))

@feature(
    sdm_web_data_with_rundate,
    feature_name=f"{run_params['prefix_name']}_desktop_user_{run_params['time_window']}days",
    description="My super feature", 
    id_column="client_id_hash",
    timeid_column="run_date",
    entity="client",
    dtype="DOUBLE",
    skip_computed=True,
    write=True,
    display=True
)
def feature_desktop_user_for_tw(df: DataFrame): # , feature_name: str

    feature_name=f"{run_params['prefix_name']}_desktop_user_{run_params['time_window']}days"

    df_web_subset_01_mobil = (
        df.select(
            "session_start_datetime", "date", "client_id_hash", "device_type", "run_date"
        )
        .filter(f.col("date") >= start_date)
        .filter(f.col("date") <= int(run_date))
    )

    # aggregation/calculation of feature
    df_web_mobile_user = (
        df_web_subset_01_mobil.drop_duplicates()
        .withColumn(
        "is_mobile",
        f.when((f.col("device_type") == "mobile"), 1)
        .when((f.col("device_type") == ("tablet"))
                | (f.col("device_type") == ("desktop")), 0
            )
        )
        .groupBy("client_id_hash", "run_date")
        .agg(
            f.round(f.avg("is_mobile"), 1).alias(feature_name)
        )
    )

    return df_web_mobile_user


@featureLoader(display=True)
def load_covid_statistics(feature_store: FeatureStore):
    return feature_store.get(entity_name='client',
                            feature_name_list=['web_analytics_desktop_user_90days'])

@featureLoader(display=True)
def load_covid_statistics(feature_store: FeatureStore):
    return feature_store.get(entity_name='client',
                            feature_name_list=['web_analytics_desktop_user_90days', 'web_analytics_desktop_user_120days'])
                        
@featureLoader(display=True)
def load_covid_statistics(feature_store: FeatureStore):
    return feature_store.get(entity_name='client')