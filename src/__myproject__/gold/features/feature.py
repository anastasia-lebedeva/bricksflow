# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass
from datalakebundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from databricksbundle.notebook.function.ArgumentsResolver import ArgumentsResolver
from databricksbundle.notebook.function.functionInspector import inspectFunction
from datalakebundle.table.TableExistenceChecker import TableExistenceChecker
from pyspark.sql.session import SparkSession

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

    def materialize_table(self, entity: Entity):

        def build_create_table_string(table_name):
            return (
                f'CREATE TABLE IF NOT EXISTS {self.db_name}.{table_name}\n'
                f'({entity.id_column_name} {entity.id_column_type} COMMENT "Entity id column",\n'
                f'{entity.timeid_column_name} {entity.timeid_column_type} COMMENT "Compute time id column")\n'
                f'USING DELTA\n'
                f'COMMENT "The table contains entity {entity.name} features"\n'
            )

        table_name = self.__get_table_name(entity.name)
        return self.__spark.sql(build_create_table_string(table_name)).collect()

    def is_materialized(self, entity_name) -> bool:
        return self.__table_existence_checker.tableExists(
            self.db_name, self.__get_table_name(entity_name)
        )
        
    def __get_table_name(self, entity_name: str) -> str:
        return f'{self.tb_name_prefix}{entity_name}'

    def __create_table(self, table_name, id_column_name, id_column_type, timestamp_column_name, timestamp_column_type):
        print("Not implemented yet")


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

        if not self.feature_store.is_materialized(entity_name):
            if not materialize_if_missing:
                raise ValueError("Entity is not materialized yet. Set materialize_if_missing = True to materialize")
            self.feature_store.materialize_table(entity)

        return entity


class feature(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, **kwargs): 
        
        self._feature_description = kwargs.get('description')
        self._entity_name = kwargs.get('entity') 
        self._feature_dtype = kwargs.get('dtype') 

    def onExecution(self, container: ContainerInterface):
        #argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)
        #arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)
        
        # TODO: move to something sm like EntityManager.get(kwargs.get('entity')) (mb in init)
        entities = container.getParameters().featurestorebundle.entities
        # e_config = e for e in entities if e.name == self._entity
        # env = os.environ.get("CURRENT_ENV")
        env = container.getParameters().kernel.environment

        entity_manager: entity_manager = container.get(EntityManager)

        # fetch entity and validate 
        entity = entity_manager.get(self._entity_name, materialize_if_missing=True)
        # TODO: something like this
        # entity.align(df_feature)

        
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
        
        print("self._entity is: " + self._entity_name)
        return None
        # call original function 
        # return super().onExecution(container)
    
    def afterExecution(self, container: ContainerInterface):
        # entityManager.makeSureTablesExists
        
        entities = container.getParameters().featurestorebundle.entities

        # do # full outer join is done

        displayFunction(self._result)
