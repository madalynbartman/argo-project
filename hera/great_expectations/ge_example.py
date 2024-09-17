from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from ruamel import yaml


# Generate a data context for ge. 
# We are going to think ahead a little and select paths that we will make available in our docker image
data_context_config = DataContextConfig(
    datasources={
        "pandas": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "PandasExecutionEngine"
            },
            data_connectors={
                "faker_data": {
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "base_directory": "/data",
                    "assets": {
                        "faker_data": {
                            "pattern": r"(.*)",
                            "group_names": ["data_asset"]
                        }
                    },
                }
            },
        )
    },
    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/ge-store"),
)

# Initialize the context.
context = BaseDataContext(project_config=data_context_config)

# Configure data sources
datasource_config = {
    "name": "faker_data",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": "/data/",
            "default_regex": {"group_names": ["data_asset_name"], "pattern": "(.*)"},
        },
    },
}

context.add_datasource(**datasource_config)

# Create a batch of data
batch_request = RuntimeBatchRequest(
    datasource_name="faker_data",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="test",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"path": "/data/faker_test.csv"},  # Add your path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# Create an expectation suite 
context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
# Run validation using the test batch from above.
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())