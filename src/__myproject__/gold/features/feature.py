# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from databricksbundle.notebook.function.ArgumentsResolver import ArgumentsResolver
from databricksbundle.notebook.function.functionInspector import inspectFunction
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from datetime import datetime


class Feature:

    def __init__(self, name: str, description: str, entity_name: str, dtype: str): 
        
        self.name = name
        self.description = description
        self.entity_name = entity_name
        self.dtype = dtype

class Entity:
    def __init__(
        self,
        name: str,
        config: str
     ):

     self.name = name 
     self.id_column_name = config['id_column_name']
     self.id_column_type = config['id_column_type']
     self.timeid_column_name = config['timeid_column_name']
     self.timeid_column_type = config['timeid_column_type']


class EntityConfigManager:

    def __init__(
        self,
        entities_config
     ):

     self.__entities_config = entities_config

    def exists(self, name: str) -> bool:
        for ec in self.__entities_config:
            if ec.name == name:
                return True
        return False

    def get(self, name: str):
        for ec in self.__entities_config:
            if ec.name == name:
                return ec
        raise Exception(f'Entity with name {name} not found among featurestorebundle.entities')

class EntityManager:

    def __init__(
        self,
        fs_db_name: str,
        tb_name_prefix: str,
        entity_config_manager: EntityConfigManager,
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession
     ):
     
     self.fs_db_name = fs_db_name
     self.tb_name_prefix = tb_name_prefix
     self.__entity_config_manager = entity_config_manager
     self.__table_existence_checker = table_existence_checker
     self.__spark = spark

    def get_full_tablename(self, entity_name: str) -> str:
        return f'{self.fs_db_name}.{self.tb_name_prefix}{entity_name}'

    def get_tablename(self, entity_name: str) -> str:
        return f'{self.tb_name_prefix}{entity_name}'

    def get(self, entity_name: str) -> Entity:
        entity_config = self.__entity_config_manager.get(entity_name)
        return Entity(entity_name, entity_config)
    
    def get_values(self, entity_name: str):
        return self.__spark.read.table(self.get_full_tablename(entity_name))

    def is_registred(self, entity_name: str) -> bool:
        return self.__table_existence_checker.tableExists(
            self.fs_db_name, self.get_tablename(entity_name)
        )
    
    def register(self, entity_name: str):
        def build_create_entity_table_string(entity: Entity):
            return (
                f'CREATE TABLE IF NOT EXISTS {self.get_full_tablename(entity.name)}\n'
                f'({entity.id_column_name} {entity.id_column_type} COMMENT "Entity id column",\n'
                f'{entity.timeid_column_name} {entity.timeid_column_type} COMMENT "Compute time id column")\n'
                f'USING DELTA\n'
                f'PARTITIONED BY ({entity.timeid_column_name})\n'
                f'COMMENT "The table contains entity {entity.name} features"\n'
        )
        entity_config = self.__entity_config_manager.get(entity_name)
        entity = Entity(entity_name, entity_config)

        return self.__spark.sql(build_create_entity_table_string(entity)).collect()

class FeatureManager:

    def __init__(
        self,
        fs_db_name: str,
        entity_manager: EntityManager,
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession
     ):
     
     self.fs_db_name = fs_db_name
     self.__entity_manager = entity_manager
     self.__table_existence_checker = table_existence_checker
     self.__spark = spark

    def register(self, feature: Feature):
        def build_add_column_string(table_name):
            return (
                f'ALTER TABLE {self.fs_db_name}.{table_name}\n'
                f'ADD COLUMNS ({feature.name} {feature.dtype} COMMENT "{feature.description}")\n'
        )
        entity_tablename = self.__entity_manager.get_tablename(feature.entity_name)
        return self.__spark.sql(build_add_column_string(entity_tablename)).collect()
    
    def is_registred(self, feature: Feature):
        entity_tablename = self.__entity_manager.get_tablename(feature.entity_name)
        for r in self.__spark.sql(f"SHOW COLUMNS IN {self.fs_db_name}.{entity_tablename}").collect():
            if r.col_name == feature.name:
                return True

        return False

    def update_metadata(self, feature: Feature):
        def build_alter_column_string(table_name):
            return (
                f'ALTER TABLE {self.fs_db_name}.{table_name}\n'
                f'ALTER COLUMN ({feature.name} {feature.dtype} COMMENT "{feature.description})"\n'
        )
        entity_tablename = self.__entity_manager.get_tablename(feature.entity_name)
        return self.__spark.sql(build_alter_column_string(entity_tablename)).collect()
    
    def get_values(self, feature: Feature):

        entity = self.__entity_manager.get(feature.entity_name)
        return (self.__entity_manager.get_values(entity.name).select(
                    feature.name,
                    entity.id_column_name,
                    entity.timeid_column_name)
        )

    def store_values(self, 
                     feature: Feature,
                     df_values: DataFrame,
                     input_df_id_column_name: str,
                     input_df_timeid_column_name: str):

        def build_merge_into_string(entity, entity_tablename, view_tablename):
            return (
                f'MERGE INTO {entity_tablename} AS fs\n'
                f'USING {view_tablename} AS newdata\n'
                f'ON fs.{entity.id_column_name} = newdata.{input_df_id_column_name} '
                f'AND fs.{entity.timeid_column_name} = newdata.{input_df_timeid_column_name}\n'
                f'WHEN MATCHED THEN\n'
                f'UPDATE SET fs.{feature.name} = newdata.{feature.name}\n'
                f'WHEN NOT MATCHED\n'
                f'THEN INSERT ({entity.id_column_name}, {entity.timeid_column_name}, {feature.name}) '
                f'VALUES ({input_df_id_column_name}, {input_df_timeid_column_name}, {feature.name})\n'
        )

        entity = self.__entity_manager.get(feature.entity_name)
        entity_tablename = self.__entity_manager.get_full_tablename(feature.entity_name)
        
        # store data to a view
        run_date = datetime.now().date().strftime("%Y%m%d")
        view_tablename = f'fv_{feature.name}_{run_date}'
        df_values.createOrReplaceTempView(view_tablename)

        return self.__spark.sql(build_merge_into_string(entity, entity_tablename, view_tablename)).collect()


class FeatureStore:

    def __init__(
        self,
        db_name: str,
        entity_manager: EntityManager,
        feature_manager: FeatureManager,
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession
     ):

        self.db_name = db_name
        self.__entity_manager = entity_manager
        self.__feature_manager = feature_manager
        self.__table_existence_checker = table_existence_checker
        self.__spark = spark

        self.__materialize_database()

    def __materialize_database(self):
        return self.__spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.db_name}').collect()
        
    def contains_feature(self, feature: Feature):
        return self.__feature_manager.is_registred(feature)
    
    def contains_entity(self, entity_name: str):
        return self.__entity_manager.is_registred(entity_name)
    
    def get_entity(self, entity_name: str):
        return self.__entity_manager.get(entity_name)

    def get_feature_values(self, feature: Feature) -> DataFrame:

        # register feature & entity if missing
        if not self.__entity_manager.is_registred(feature.entity_name):
            self.__entity_manager.register(feature.entity_name)
        if not self.__feature_manager.is_registred(feature):
            self.__feature_manager.register(feature)

        return self.__feature_manager.get_values(feature)

    def store_values(self, feature: Feature,
                           df_new_values: DataFrame,
                           input_df_id_column_name: str,
                           input_df_timeid_column_name: str):
        
        self.__feature_manager.store_values(feature,
                                            df_new_values,
                                            input_df_id_column_name,
                                            input_df_timeid_column_name)


class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, **kwargs): 
        
        self.__feature = Feature(name = kwargs.get('feature_name'),
                               description=kwargs.get('description'),
                               entity_name=kwargs.get('entity'),
                               dtype=kwargs.get('dtype'))

        self.__df = args[0]._result
        self.__df_id_column = kwargs.get('id_column')
        self.__df_timeid_column = kwargs.get('timeid_column')

    def __filter_computed_values(self,
                                df_feature_cache: DataFrame,
                                df_feature_id_column_name: str,
                                df_feature_timeid_column_name: str
                                ) -> DataFrame:
        return (
            self.__df
            .alias("df_input")
            .join(
                df_feature_cache.alias("df_feature"),
                ((col(f"df_input.{self.__df_id_column}") == col(f"df_feature.{df_feature_id_column_name}")) & \
                 (col(f"df_input.{self.__df_timeid_column}") == col(f"df_feature.{df_feature_timeid_column_name}"))),
                "left"
            )
            .where(col(self.__feature.name).isNull())
            .select("df_input.*")
        )
    
    def __build_result_feature_df(self,
                                df_feature_values: DataFrame,
                                df_feature_id_column_name: str,
                                df_feature_timeid_column_name: str
                                )-> DataFrame:
        return (
            self.__df
            .alias("df_result")
            .select([self.__df_id_column, self.__df_timeid_column])
            .drop_duplicates()
            .join(df_feature_values.alias("df_feature"),
                ((col(f"df_result.{self.__df_id_column}") == col(f"df_feature.{df_feature_id_column_name}")) & \
                 (col(f"df_result.{self.__df_timeid_column}") == col(f"df_feature.{df_feature_timeid_column_name}"))),
                "left"
            )
            .select("df_result.*", f"df_feature.{self.__feature.name}")
            .where(col(f"df_feature.{self.__feature.name}").isNotNull())
        )


    def onExecution(self, container: ContainerInterface):

        feature_store: feature_store = container.get(FeatureStore)
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)

        # get already computed values from cache
        df_feature_cache = feature_store.get_feature_values(feature=self.__feature)
        entity = feature_store.get_entity(entity_name=self.__feature.entity_name)

        # get only values to compute
        df_to_compute = self.__filter_computed_values(df_feature_cache,
                                                      entity.id_column_name,
                                                      entity.timeid_column_name)

        # call function for not computed only 
        functions_args = inspectFunction(self._function)
        arguments = argumentsResolver.resolve(functions_args, self._decoratorArgs)
        arguments_list = list(arguments); arguments_list[0] = df_to_compute
        arguments = tuple(arguments_list)

        return self._function(*arguments)

    
    def afterExecution(self, container: ContainerInterface):
        
        feature_store: feature_store = container.get(FeatureStore)
        
        feature_store.store_values(feature=self.__feature, 
                                   df_new_values=self._result,
                                   input_df_id_column_name=self.__df_id_column,
                                   input_df_timeid_column_name=self.__df_timeid_column)

        df_feature_values = feature_store.get_feature_values(feature=self.__feature)
        entity = feature_store.get_entity(entity_name=self.__feature.entity_name)

        # get values for all given id & timestamp
        self._result = self.__build_result_feature_df(df_feature_values,
                                                      entity.id_column_name,
                                                      entity.timeid_column_name)

        displayFunction(self._result)

