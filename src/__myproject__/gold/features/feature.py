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


class Feature:

    def __init__(self, name: str, description: str, entity_name: str, dtype: str): 
        
        self.name = name
        self.description = description
        self.entity_name = entity_name
        self.feature_dtype = dtype

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

    def __get(self, entity_name: str) -> Entity:
        
        entity_config = self.__entity_config_manager.get(entity_name)
        return Entity(entity_name, entity_config)

    def get_full_tablename(self, entity_name: str) -> str:
        return f'{self.fs_db_name}.{self.tb_name_prefix}{entity_name}'

    def get_tablename(self, entity_name: str) -> str:
        return f'{self.tb_name_prefix}{entity_name}'

    def get(self, entity_name: str, register_if_missing: bool) -> Entity:

        entity = self.__get(entity_name)
        if not self.is_registred(entity_name):
            if not register_if_missing:
                raise ValueError("Entity is not registred yet. Set register_if_missing = True to materialize")
            self.register(entity)

        return entity
    
    def get_values(self, entity_name: str):
        return self.__spark.read.table(self.get_full_tablename(entity_name))

    def is_registred(self, entity_name: str) -> bool:
        return self.__table_existence_checker.tableExists(
            self.fs_db_name, self.get_tablename(entity_name)
        )
    
    def register(self, entity: Entity):
        def build_create_table_string(table_name):
            return (
                f'CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name}\n'
                f'({entity.id_column_name} {entity.id_column_type} COMMENT "Entity id column",\n'
                f'{entity.timeid_column_name} {entity.timeid_column_type} COMMENT "Compute time id column")\n'
                f'USING DELTA\n'
                f'PARTITIONED BY ({entity.timeid_column_name})\n'
                f'COMMENT "The table contains entity {entity.name} features"\n'
        )
        table_name = self.__get_entity_table_name(entity.name)
        return self.__spark.sql(build_create_table_string(table_name)).collect()

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
                f'ADD COLUMNS ({feature.name} {feature.dtype} COMMENT "{feature.description})"\n'
        )
        table_name = self.__get_table_name(feature.entity_name)
        return self.__spark.sql(build_add_column_string(table_name)).collect()
    
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
    
    def register_feature(self, feature: Feature):
        return self.__feature_manager.register(feature)
    
    def contains_entity(self, entity_name: str):
        return self.__entity_manager.is_registred(entity_name)

    def join_with_feature_cache(self,
                                feature: Feature,
                                df_input: DataFrame,
                                input_df_id_column_name: str,
                                input_df_timeid_column_name: str) -> DataFrame:

        # fetch entity, register if is missing 
        entity = self.__entity_manager.get(feature.entity_name, register_if_missing=True)

        # return all None if feature is not registered yet
        if not self.__feature_manager.is_registred(feature):
            return df_input.withColumn(feature.name, lit(None))

        # join with computed values of feature os registered
        df_cache_feature = (self.__entity_manager.get_values(entity.name)
            .select(
                feature.name,
                entity.id_column_name,
                entity.timeid_column_name
            )
        ) 

        df_join = (
            df_input.join(df_cache_feature,
                (df_input(input_df_id_column_name) == df_cache_feature(entity.id_column_name) & \
                df_input(input_df_timeid_column_name) == df_cache_feature(entity.timeid_column_name)),
                "left"
            )
        )

        return df_join

    #def store_values(self, feature: Feature,
    #                       entity: Entity,
    #                       df: DataFrame,
    #                       input_df_id_column_name: str,
    #                       input_df_timeid_column_name: str):
    #    pass


class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, **kwargs): 
        
        self._feature = Feature(name = kwargs.get('feature_name'),
                               description=kwargs.get('description'),
                               entity_name=kwargs.get('entity'),
                               dtype=kwargs.get('dtype'))

        self._df = args[0]._result
        self._df_id_column = kwargs.get('id_column')
        self._df_timeid_column = kwargs.get('timeid_column')

    def onExecution(self, container: ContainerInterface):

        feature_store: feature_store = container.get(FeatureStore)
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)

        # get already computed values from cache
        df_input_cache_join = feature_store.join_with_feature_cache(feature=self._feature, 
                                                                    df_input=self._df,
                                                                    input_df_id_column_name=self._df_id_column,
                                                                    input_df_timeid_column_name=self._df_timeid_column)

        df_computed = df_input_cache_join.where(col(self._feature.name).isNotNull())
        df_to_compute = df_input_cache_join.where(col(self._feature.name).isNull())

        # call function for not computed only 
        arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)
        arguments_list = list(arguments); arguments_list[0] = df_to_compute
        arguments = tuple(arguments_list)

        return self._function(*arguments)

    
    def afterExecution(self, container: ContainerInterface):
        
        feature_store: feature_store = container.get(FeatureStore)
        
        if not feature_store.contains_feature(self._feature):
            feature_store.register_feature(self._feature)
        
        #df_output = feature_store.store_values(feature=self._feature, 
        #                                       df=self._result,
        #                                       input_df_id_column_name=self._df_id_column,
        #                                       input_df_timeid_column_name=self._df_timeid_column)


        displayFunction(self._result)
