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

    def __init__(self, name: str, description: str, dtype: str): 
        
        self.name = name
        self.description = description
        self.dtype = dtype

class Entity:
    def __init__(
        self,
        name: str,
        id_column_name: str, 
        id_column_type: str, 
        timeid_column_name: str,
        timeid_column_type: str 
     ):

     self.name = name 
     self.id_column_name = id_column_name
     self.id_column_type = id_column_type
     self.timeid_column_name = timeid_column_name
     self.timeid_column_type = timeid_column_type

class EntityManager:

    def __init__(
        self,
        fs_db_name: str,
        tb_name_prefix: str,
        table_existence_checker: TableExistenceChecker,
        spark: SparkSession
     ):
     
     self.fs_db_name = fs_db_name
     self.tb_name_prefix = tb_name_prefix
     self.__table_existence_checker = table_existence_checker
     self.__spark = spark

    def get_full_tablename(self, entity_name: str) -> str:
        return f'{self.fs_db_name}.{self.tb_name_prefix}{entity_name}'

    def get_tablename(self, entity_name: str) -> str:
        return f'{self.tb_name_prefix}{entity_name}'

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
    
    def register(self, entity: Entity):
        def build_create_entity_table_string(entity: Entity):
            return (
                f'CREATE TABLE IF NOT EXISTS {self.get_full_tablename(entity.name)}\n'
                f'({entity.id_column_name} {entity.id_column_type} COMMENT "Entity id column",\n'
                f'{entity.timeid_column_name} {entity.timeid_column_type} COMMENT "Compute time id column")\n'
                f'USING DELTA\n'
                f'PARTITIONED BY ({entity.timeid_column_name})\n'
                f'COMMENT "The table contains entity {entity.name} features"\n'
        )
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

    def register(self, entity: Entity, feature_name: str, feature_description:str, feature_dtype:str):
        def build_add_column_string(table_name):
            return (
                f'ALTER TABLE {self.fs_db_name}.{table_name}\n'
                f'ADD COLUMNS ({feature_name} {feature_dtype} COMMENT "{feature_description}")\n'
        )
        entity_tablename = self.__entity_manager.get_tablename(entity.name)
        return self.__spark.sql(build_add_column_string(entity_tablename)).collect()
    
    # TODO: is not used yet
    def update_metadata(self, entity: Entity, feature_name: str, feature_description:str, feature_dtype:str):
        def build_alter_column_string(table_name):
            return (
                f'ALTER TABLE {self.fs_db_name}.{table_name}\n'
                f'ALTER COLUMN ({feature_name} {feature_dtype} COMMENT "{feature_description})"\n'
        )
        entity_tablename = self.__entity_manager.get_tablename(entity.name)
        return self.__spark.sql(build_alter_column_string(entity_tablename)).collect()
    

    def is_registred(self, feature_name: str, entity_name:str):

        if not self.__entity_manager.is_registred(entity_name):
            return False
        return feature_name in \
                self.__entity_manager.get_registred_feature_names(entity_name)

    def get_values(self, entity_name: str, feature_name: List[str]=None):
        
        df = self.__entity_manager.get_values(entity_name)
        if not feature_name:
            return df
        return df.select(df.columns[:2] + feature_name)   

    def update_existing_insert_new_values(self, 
                                        entity: Entity,
                                        feature_name_list: List[str],
                                        df_values: DataFrame,
                                        input_df_id_column_name: str,
                                        input_df_timeid_column_name: str):

        def build_update_set_string():
            update_set_str = f'UPDATE SET fs.{feature_name_list[0]} = newdata.{feature_name_list[0]}'
            for f_name in feature_name_list[1:]:
                update_set_str+=f', fs.{f_name} = newdata.{f_name}'
            return update_set_str
        
        def build_insert_feature_colstring():
            return ','.join(feature_name_list)
            
        def build_merge_into_string(entity, entity_tablename, view_tablename):
            return (
                f'MERGE INTO {entity_tablename} AS fs\n'
                f'USING {view_tablename} AS newdata\n'
                f'ON fs.{entity.id_column_name} = newdata.{input_df_id_column_name} '
                f'AND fs.{entity.timeid_column_name} = newdata.{input_df_timeid_column_name}\n'
                f'WHEN MATCHED THEN\n'
                f'{build_update_set_string()}\n'
                f'WHEN NOT MATCHED\n'
                f'THEN INSERT ({entity.id_column_name}, {entity.timeid_column_name}, {build_insert_feature_colstring()}) '
                f'VALUES ({input_df_id_column_name}, {input_df_timeid_column_name}, {build_insert_feature_colstring()})\n'
        )

        entity_tablename = self.__entity_manager.get_full_tablename(entity.name)
        
        # store data to a view
        run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        view_tablename = f'fv_{run_timestamp}'
        df_values.createOrReplaceTempView(view_tablename)
        merge_str = build_merge_into_string(entity, entity_tablename, view_tablename)

        return self.__spark.sql(merge_str).collect()


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

    def __register(self, feature_name: str, feature_description:str, feature_dtype:str, entity: Entity): 
        if not self.__entity_manager.is_registred(entity.name):
            self.__entity_manager.register(entity)
        if not self._contains_feature(feature_name, entity.name):
            self.__feature_manager.register(entity, feature_name, feature_description, feature_dtype)    

    def _contains_feature(self, feature_name: str, entity_name: str):
        return self.__feature_manager.is_registred(feature_name, entity_name)
    
    def update(
        self,
        entity: Entity,
        feature_name_list: List[str],
        feature_description_list: List[str],
        feature_dtype_list: List[str],
        df_new_values: DataFrame,
        input_df_id_column_name: str,
        input_df_timeid_column_name: str
    ):

        for i in range(len(feature_name_list)):
            if self._contains_feature(feature_name_list[i], entity.name):
                 continue
            
            # register missing 
            self.__register(
                feature_name_list[i],
                feature_description_list[i],
                feature_dtype_list[i],
                entity)

        self.__feature_manager.update_existing_insert_new_values(
            entity,
            feature_name_list,
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
    
    def get_for_id_timeid(self,
            entity_name: str,
            df_id_timeid: DataFrame,
            df_id_column_name: str,
            df_timeid_column_name: str,
            feature_name_list: List[str]=None
        ):

        feature_name_list = feature_name_list or []
        df_feature = self.get(entity_name, feature_name_list)

        df_join = (
            df_id_timeid
            .alias("df_id")
            .join(
                df_feature.alias("df_feature"),
                ((col(f"df_id.{df_id_column_name}") == col(f"df_feature.{df_feature.columns[0]}")) & \
                 (col(f"df_id.{df_timeid_column_name}") == col(f"df_feature.{df_feature.columns[1]}"))),
                "left"
            )
            .select(["df_id.*"] + feature_name_list)
        )

        return df_join

class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(
        self,
        *args,
        feature_name:str,
        description:str, 
        dtype: str, 
        entity_name: str,
        fs_id_column_name: str, 
        fs_id_column_type: str, 
        fs_timeid_column_name: str, 
        fs_timeid_column_type: str, 
        df_id_column: str, 
        timeid_column: str, 
        write: bool,
        skip_computed: bool=False,
        display: bool=False
    ): 

        if not (type(feature_name) == type(description) and type(description) == type(dtype)):
            raise("Type of feature_name, description and dtype args should be the same")
        if not type(feature_name) in set([str, list]):
            raise("Type of feature_name, description and dtype args should be either string or list")
        
        if type(feature_name) == str:
            feature_name = [feature_name]
            description = [description]
            dtype = [dtype]
        
        self.__feature_name_list = feature_name
        self.__feature_description_list = description
        self.__feature_dtype_list = dtype

        self.__entity = Entity(
            name=entity_name,
            id_column_name=fs_id_column_name,
            id_column_type=fs_id_column_type,
            timeid_column_name=fs_timeid_column_name,
            timeid_column_type=fs_timeid_column_type,
        )                       

        self.__df = args[0]._result
        self.__df_id_column = df_id_column
        self.__df_timeid_column = timeid_column
        self.__skip_computed = skip_computed
        self.__display = display
        self.__write = write

    def onExecution(self, container: ContainerInterface):

        if not self.__write and self.__skip_computed:
            raise ValueError("Option when write=False and skip_computed=True is not implemented yet")

        feature_store: FeatureStore = container.get(FeatureStore)
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)
        arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)

        # proceed if a feature is not registred
        for f_name in self.__feature_name_list:
            if not feature_store._contains_feature(f_name, self.__entity.name):
                return self._function(*arguments)
        
        # proceeed of requested in decorator args
        if not self.__skip_computed:
            return self._function(*arguments)
        
        # get feature values for given ids 
        df_for_id_timeid = feature_store.get_for_id_timeid(
            entity_name=self.__entity.name,
            df_id_timeid=self.__df,
            df_id_column_name=self.__df_id_column,
            df_timeid_column_name=self.__df_timeid_column,
            feature_name_list=self.__feature_name_list
        )

        # get only column from input df with at least one null
        df_to_compute = (df_for_id_timeid
            .withColumn('num_nulls', sum(col(f_col).isNull().cast('int') for f_col in self.__feature_name_list))
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
                entity=self.__entity,
                feature_name_list=self.__feature_name_list,
                feature_description_list=self.__feature_description_list,
                feature_dtype_list=self.__feature_dtype_list,
                df_new_values=self._result,
                input_df_id_column_name=self.__df_id_column,
                input_df_timeid_column_name=self.__df_timeid_column
            )

            df_id_timeid_input = (
                self.__df
                .select([self.__df_id_column, self.__df_timeid_column])
                .drop_duplicates()
            )

            self._result = feature_store.get_for_id_timeid(
                entity_name=self.__entity.name,
                df_id_timeid=df_id_timeid_input,
                df_id_column_name=self.__df_id_column,
                df_timeid_column_name=self.__df_timeid_column,
                feature_name_list=self.__feature_name_list
            )

        if self.__display and container.getParameters().datalakebundle.notebook.display.enabled is True:
            displayFunction(self._result)

class clientFeature(feature):

    # fails as DecoratorMetaclass required __init__
    # entity = 'client'
    # id_column = 'client_id_hash'
    
    ENTITY_NAME = 'client'
    DF_ID_COLUMN = 'client_id_hash'
    FS_ID_COLUMN_NAME = 'client_id_hash'
    FS_ID_COLUMN_TYPE = 'STRING'
    FS_TIMEID_COLUMN_NAME = 'trigger_timestamp'
    FS_TIMEID_COLUMN_TYPE = 'INT'

    def __init__(
        self,
        *args,
        **kwargs
    ):

        super().__init__( 
            *args,
            **kwargs,
            entity_name = clientFeature.ENTITY_NAME, 
            df_id_column = clientFeature.DF_ID_COLUMN,
            fs_id_column_name = clientFeature.FS_ID_COLUMN_NAME,
            fs_id_column_type = clientFeature.FS_ID_COLUMN_TYPE,
            fs_timeid_column_name = clientFeature.FS_TIMEID_COLUMN_NAME,
            fs_timeid_column_type = clientFeature.FS_TIMEID_COLUMN_TYPE
        )

class featureLoader(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, display=False): # pylint: disable = unused-argument
        self._display = display

    def afterExecution(self, container: ContainerInterface):
        if self._display and container.getParameters().datalakebundle.notebook.display.enabled is True:
            displayFunction(self._result)