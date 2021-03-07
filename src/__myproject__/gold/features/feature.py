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


class FeatureStore:

    def __init__(
        self,
        environment: str,
        tb_name_prefix: str,
        db_name_suffix: str,
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession
     ):

        self.environment = environment
        self.tb_name_prefix = tb_name_prefix
        self.db_name_suffix = db_name_suffix
        self.db_name = f'{self.environment}{self.db_name_suffix}'
        self.__table_existence_checker = table_existence_checker
        self.__spark = spark

        self.__materialize_database()

    def __materialize_database(self):
        return self.__spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.db_name}').collect()
        
    def __get_table_fullname(self, entity_name: str) -> str:
        return f'{self.db_name}.{self.__get_table_name(entity_name)}'

    def __get_table_name(self, entity_name: str) -> str:
        return f'{self.tb_name_prefix}{entity_name}'

    def __get_entity_df(self, entity_name: str):
        return self.__spark.read.table(self.__get_table_fullname(entity_name))

    #def __get_table_feature_list(self, entity_name: str):
    #    get_columns_query = f"SHOW COLUMNS IN {self.__get_table_fullname(entity_name)}"
    #    colname_list = [r.col_name for r in self.__spark.sql(get_columns_query).collect()]

    #    return colname_list
    
    def __table_has_column(self, col_name: str, table_fullname: str):
        get_columns_query = f"SHOW COLUMNS IN {table_fullname}"
        for r in self.__spark.sql(get_columns_query).collect():
            if r.col_name == col_name:
                return True

        return False
    
    # TODO: generalize function input
    def __materialize_table(self, entity: Entity):
        def build_create_table_string(table_name):
            return (
                f'CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name}\n'
                f'({entity.id_column_name} {entity.id_column_type} COMMENT "Entity id column",\n'
                f'{entity.timeid_column_name} {entity.timeid_column_type} COMMENT "Compute time id column")\n'
                f'USING DELTA\n'
                f'PARTITIONED BY ({entity.timeid_column_name})\n'
                f'COMMENT "The table contains entity {entity.name} features"\n'
        )
        table_name = self.__get_table_name(entity.name)
        return self.__spark.sql(build_create_table_string(table_name)).collect()

    def materialize_entity(self, entity: Entity):
        self.__materialize_table(entity)

    def contains_entity(self, entity_name) -> bool:
        return self.__table_existence_checker.tableExists(
            self.db_name, self.__get_table_name(entity_name)
        )  

    def contains_feature(self, feature_name: str, entity_name: str):
        return self.__table_has_column(
            feature_name, self.__get_table_fullname(entity_name)
        )

    def align_with_cache(self,
                         feature: Feature,
                         entity: Entity,
                         df_input: DataFrame,
                         input_df_id_column_name: str,
                         input_df_timeid_column_name: str) -> DataFrame:

        # feature is not registered yet
        if not self.contains_feature(feature.name, entity.name):
            return df_input.withColumn(feature.name, lit(None))

        # feature is registered
        df_cache_feature = (
            self.__get_entity_df(entity.name)
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

    def register_feature(self, feature: Feature):
        def build_add_column_string(table_name):
            return (
                f'ALTER TABLE {self.db_name}.{table_name}\n'
                f'ADD COLUMNS ({feature.name} {feature.dtype} COMMENT "{feature.description})"\n'
        )
        table_name = self.__get_table_name(feature.entity_name)
        return self.__spark.sql(build_add_column_string(table_name)).collect()

    def update_feature_metadata(self, feature: Feature):
        def build_add_column_string(table_name):
            return (
                f'ALTER TABLE {self.db_name}.{table_name}\n'
                f'ALTER COLUMN ({feature.name} {feature.dtype} COMMENT "{feature.description}"\n'
        )
        table_name = self.__get_table_name(feature.entity_name)
        return self.__spark.sql(build_add_column_string(table_name)).collect()
    
    def store_values(self, feature: Feature,
                           entity: Entity,
                           df: DataFrame,
                           input_df_id_column_name: str,
                           input_df_timeid_column_name: str):
        passs



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
        entity_config_manager: EntityConfigManager,
        feature_store: FeatureStore
     ):

     self.__entity_config_manager = entity_config_manager
     self.feature_store = feature_store

    def __get(self, entity_name: str) -> Entity:
        
        entity_config = self.__entity_config_manager.get(entity_name)
        return Entity(entity_name, entity_config)

    def get(self, entity_name: str, materialize_if_missing: bool) -> Entity:

        entity = self.__get(entity_name)

        if not self.feature_store.contains_entity(entity_name):
            if not materialize_if_missing:
                raise ValueError("Entity is not materialized yet. Set materialize_if_missing = True to materialize")
            self.feature_store.materialize_entity(entity)

        return entity

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
        #argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)
        #arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)
        
        # TODO: move to something sm like EntityManager.get(kwargs.get('entity')) (mb in init)
        # entities = container.getParameters().featurestorebundle.entities
        # e_config = e for e in entities if e.name == self._entity
        # env = os.environ.get("CURRENT_ENV")
        # env = container.getParameters().kernel.environment

        entity_manager: entity_manager = container.get(EntityManager)
        feature_store: feature_store = container.get(FeatureStore)
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)

        # fetch entity
        entity = entity_manager.get(self._feature.entity_name, materialize_if_missing=True)

        df_input_cache_join = feature_store.align_with_cache(feature=self._feature, 
                                                       entity=entity,
                                                       df_input=self._df,
                                                       input_df_id_column_name=self._df_id_column,
                                                       input_df_timeid_column_name=self._df_timeid_column)

        df_computed = df_input_cache_join.where(col(self._feature.name).isNotNull())
        df_to_compute = df_input_cache_join.where(col(self._feature.name).isNull())
        
        # check entity configuration exists 
        # check data congiguration is valid for the entity 
        # check ids are unique 
        # checks there are None in id
        
        # if ok
        # create or get the entity based on configuration
        # 
        # read table and check if 
        # (1) feature is in place - if not, skeep the next check
        # (2) ids & timestamps are already there - 
        #     if some are, fetch them from FS and filter them from the provided DF to apply
        
        # apply function to filtered data 
        
        arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)
        arguments_list = list(arguments); arguments_list[0] = df_to_compute
        arguments = tuple(arguments_list)

        return self._function(*arguments)

        # print("self._entity is: " + self._entity_name)
        # return super().onExecution(container)
        # call original function 
        # return super().onExecution(container)
    
    def afterExecution(self, container: ContainerInterface):
        
        entity_manager: entity_manager = container.get(EntityManager)
        feature_store: feature_store = container.get(FeatureStore)
        entity = entity_manager.get(self._feature.entity_name)
        
        if not feature_store.contains_feature(self._feature.name, entity.name):
            feature_store.register_feature(self._feature)
        
        df_output = feature_store.store_values(feature=self._feature, 
                                               entity=self.entity,
                                               df=self._result,
                                               input_df_id_column_name=self._df_id_column,
                                               input_df_timeid_column_name=self._df_timeid_column)


        displayFunction(self._result)
