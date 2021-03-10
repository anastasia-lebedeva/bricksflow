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
from typing import List


class Feature:

    def __init__(self, name: str, entity_name: str, description: str, dtype: str): 
        
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
    
    def get_registred_feature_names(self, entity_name: str):
        tname = self.get_full_tablename(entity_name)
        col_objects = self.__spark.sql(f"SHOW COLUMNS IN {tname}").collect()
        return set([r.col_name for r in col_objects])

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
    
    def is_registred(self, feature_name: str, entity_name:str):

        if not self.__entity_manager.is_registred(entity_name):
            return False

        return feature_name in \
                self.__entity_manager.get_registred_feature_names(entity_name)

    def update_metadata(self, feature: Feature):
        def build_alter_column_string(table_name):
            return (
                f'ALTER TABLE {self.fs_db_name}.{table_name}\n'
                f'ALTER COLUMN ({feature.name} {feature.dtype} COMMENT "{feature.description})"\n'
        )
        entity_tablename = self.__entity_manager.get_tablename(feature.entity_name)
        return self.__spark.sql(build_alter_column_string(entity_tablename)).collect()
    
    def get_values(self, entity_name:str, feature_name: List[str]=None):

        entity = self.__entity_manager.get(entity_name)
        if not feature_name:
            return self.__entity_manager.get_values(entity.name)

        return (
            self.__entity_manager.get_values(entity.name)
            .select(
                [entity.id_column_name,
                entity.timeid_column_name] + \
                feature_name
            )
        )     

    def update_existing_insert_new_values(self, 
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

    def __register(self, feature: Feature): 
        if not self.__entity_manager.is_registred(feature.entity_name):
            self.__entity_manager.register(feature.entity_name)
        if not self._contains_feature(feature.name, feature.entity_name):
            self.__feature_manager.register(feature)    

    def _contains_feature(self, feature_name: str, entity_name: str):
        return self.__feature_manager.is_registred(feature_name, entity_name)
    
    def update(self, feature: Feature, df_new_values: DataFrame,
            input_df_id_column_name: str, input_df_timeid_column_name: str):

        if not self._contains_feature(feature.name, feature.entity_name):
            self.__register(feature)

        self.__feature_manager.update_existing_insert_new_values(
            feature,
            df_new_values,
            input_df_id_column_name,
            input_df_timeid_column_name
        )

    def get(self, entity_name: str, feature_name_list: List[str]=None):

        feature_name_list = feature_name_list or []

        for feature_name in feature_name_list:
            if not self._contains_feature(feature_name, entity_name):
                raise ValueError(f"Feature with name {feature_name} is not registred for entity {entity_name}")

        return self.__feature_manager.get_values(entity_name, feature_name_list)
    
    def _get_for_id_timeid(self,
             entity_name: str,
            df_id_timeid: DataFrame,
            df_id_column_name: str,
            df_timeid_column_name: str,
            feature_name_list: List[str]
        ):

        entity = self.__entity_manager.get(entity_name)
        df_feature = self.get(entity_name, feature_name_list)

        df_join = (
            df_id_timeid
            .alias("df_id")
            .join(
                df_feature.alias("df_feature"),
                ((col(f"df_id.{df_id_column_name}") == col(f"df_feature.{entity.id_column_name}")) & \
                    (col(f"df_id.{df_timeid_column_name}") == col(f"df_feature.{entity.timeid_column_name}"))),
                "left"
            )
            .select(["df_id.*"] + feature_name_list)
        )

        return df_join

class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, **kwargs): # TODO: add explicit 

        self.__feature = Feature(name = kwargs.get('feature_name'),
                               entity_name=kwargs.get('entity'),
                               description=kwargs.get('description'),
                               dtype=kwargs.get('dtype'))

        self.__df = args[0]._result
        self.__df_id_column = kwargs.get('id_column')
        self.__df_timeid_column = kwargs.get('timeid_column')
        self.__skip_computed = kwargs.get('skip_computed', False)
        self.__display = kwargs.get('display', False)
        self.__write = kwargs.get('write')

    def onExecution(self, container: ContainerInterface):

        if not self.__write and self.__skip_computed:
            raise ValueError("Option when write=False and skip_computed=True is not implemented yet")

        feature_store: FeatureStore = container.get(FeatureStore)
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)
        arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)

        # proceed if feature is not registred
        if not feature_store._contains_feature(self.__feature.name, self.__feature.entity_name):
            return self._function(*arguments)
        
        # proceeed of requested in decorator args
        if not self.__skip_computed:
            return self._function(*arguments)
        
        # get feature values for given ids 
        df_for_id_timeid = feature_store._get_for_id_timeid(
            entity_name=self.__feature.entity_name,
            df_id_timeid=self.__df,
            df_id_column_name=self.__df_id_column,
            df_timeid_column_name=self.__df_timeid_column,
            feature_name_list=[self.__feature.name]
        )

        # get only column from input df with at least one null
        df_to_compute = (df_for_id_timeid
            .withColumn('num_nulls', sum(col(f_col).isNull().cast('int') for f_col in [self.__feature.name]))
            .where(col('num_nulls') > 0) 
            .select("df_id.*")
        )

        # add modified df as function argument
        arguments_list = list(arguments)
        arguments_list[0] = df_to_compute
        arguments = tuple(arguments_list)

        return self._function(*arguments)

    def afterExecution(self, container: ContainerInterface):
        
        feature_store: FeatureStore = container.get(FeatureStore)
        
        if self.__write:
            feature_store.update(
                feature=self.__feature, 
                df_new_values=self._result,
                input_df_id_column_name=self.__df_id_column,
                input_df_timeid_column_name=self.__df_timeid_column
            )

            df_id_timeid_input = (
                self.__df
                .select([self.__df_id_column, self.__df_timeid_column])
                .drop_duplicates()
            )

            self._result = feature_store._get_for_id_timeid(
                entity_name=self.__feature.entity_name,
                df_id_timeid=df_id_timeid_input,
                df_id_column_name=self.__df_id_column,
                df_timeid_column_name=self.__df_timeid_column,
                feature_name_list=[self.__feature.name]
            )

        if self.__display and container.getParameters().datalakebundle.notebook.display.enabled is True:
            displayFunction(self._result)

class clientFeature(feature, metaclass=DecoratorMetaclass):

    def __init__(
        self,
        *args,
        **kwargs
    ):

        super().__init__( 
            *args,
            **kwargs,
            entity = 'client', 
            id_column = 'client_id_hash'
        )

class featureLoader(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, display=False): # pylint: disable = unused-argument
        self._display = display

    def afterExecution(self, container: ContainerInterface):
        if self._display and container.getParameters().datalakebundle.notebook.display.enabled is True:
            displayFunction(self._result)