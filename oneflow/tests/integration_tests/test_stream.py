import pandas as pd
import logging
import os
import shutil
from oneflow.core.task.spark import SparkTask
from oneflow.core.schemas.input import SparkInput
from oneflow.core.schemas.output import SparkOutput
from oneflow.core.schemas.sources import WriteOptions
from oneflow.core.schemas.features import (
    CreateDataObjectIfNotExists,
    OutputFeatureOptions,
    InputFeatureOptions,
    SchemaInferenceFromRegistry,
)
from oneflow.core.schemas.data_objects import Table
from oneflow.core.schemas.execution import SparkExecution
from oneflow.core.schemas.refresh import TaskRefreshPolicy as SparkTaskRefreshPolicy
from pyspark.sql import DataFrame
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual

logging.basicConfig(
    format="%(levelname)s:%(created)f:%(funcName)s:%(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def sample_execution_function(data: DataFrame):
    return data


def test_stream_delta_source_delta_sink_append(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read in stream mode and writing to delta target with append write operation
    """
    current_test_folder = f"{setup_teardown}/test_stream_delta_source_delta_sink_append"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_pd = pd.DataFrame(sample_data.data)

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="append",
        sink="delta",
    )

    refresh_policy = SparkTaskRefreshPolicy(type="stream")
    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest102")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_stream_delta_source_delta_sink_overwrite(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with overwrite write operation
    """
    current_test_folder = (
        f"{setup_teardown}/test_stream_delta_source_delta_sink_overwrite"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_pd = pd.DataFrame(sample_data.data)

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="overwrite",
        sink="delta",
    )
    refresh_policy = SparkTaskRefreshPolicy(type="stream")
    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest102")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_stream_delta_source_delta_sink_scd1(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with scd1 write operation
    """
    current_test_folder = f"{setup_teardown}/test_stream_delta_source_delta_sink_scd1"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_pd = pd.DataFrame(sample_data.data)

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(
            **sample_data.create_table(f"{current_test_folder}/target/", "scd1")
        )
    )
    write_options = WriteOptions(key_attributes="id,offset")
    output_features = OutputFeatureOptions(
        create_data_object_if_not_exists=create_table_feature
    )
    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="scd1",
        sink="delta",
        options=write_options,
        features=output_features,
    )

    refresh_policy = SparkTaskRefreshPolicy(type="stream")

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest103")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setSparkconfig(
            "spark.databricks.delta.legacy.allowAmbiguousPathsInCreateTable", "true"
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(
            *[
                "__metadata_valid_to_ts__",
                "__metadata_key_hash__",
                "__metadata_data_hash__",
                "__metadata_valid_from_ts__",
            ]
        )
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("append").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name"]
    )
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(
            *[
                "__metadata_valid_to_ts__",
                "__metadata_key_hash__",
                "__metadata_data_hash__",
                "__metadata_valid_from_ts__",
            ]
        )
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_stream_delta_source_delta_sink_scd2(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with scd2 write operation
    """
    current_test_folder = f"{setup_teardown}/test_stream_delta_source_delta_sink_scd2"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_pd = pd.DataFrame(sample_data.data)

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
    )
    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(
            **sample_data.create_table(f"{current_test_folder}/target/", "scd2")
        )
    )
    write_options = WriteOptions(key_attributes="id,offset")
    output_features = OutputFeatureOptions(
        create_data_object_if_not_exists=create_table_feature
    )
    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="scd2",
        sink="delta",
        options=write_options,
        features=output_features,
    )
    refresh_policy = SparkTaskRefreshPolicy(type="stream")
    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest104")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setSparkconfig(
            "spark.databricks.delta.legacy.allowAmbiguousPathsInCreateTable", "true"
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )
    target_data_ = target_data.filter("__metadata_active__='Y'").drop(
        *[
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_active__",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("append").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name"]
    )
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )

    target_data_ = target_data.filter("__metadata_active__='Y'").drop(
        *[
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_active__",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    # checking the inactive rows for source and target
    source_data_ = source_data
    target_data_ = target_data.filter("__metadata_active__='Y'").drop(
        *[
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_active__",
        ]
    )
    assertDataFrameEqual(source_data_, target_data_)


def test_stream_delta_source_delta_sink_scd3(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with scd3 write operation
    """
    current_test_folder = f"{setup_teardown}/test_stream_delta_source_delta_sink_scd3"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_pd = pd.DataFrame(sample_data.data)

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(
            **sample_data.create_table(f"{current_test_folder}/target/", "scd3")
        )
    )
    write_options = WriteOptions(key_attributes="id", column_attributes="name,offset")
    output_features = OutputFeatureOptions(
        create_data_object_if_not_exists=create_table_feature
    )
    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="scd3",
        sink="delta",
        options=write_options,
        features=output_features,
    )
    refresh_policy = SparkTaskRefreshPolicy(type="stream")
    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest104")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setSparkconfig(
            "spark.databricks.delta.legacy.allowAmbiguousPathsInCreateTable", "true"
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )

    target_data_ = target_data.drop(
        *[
            "prev_name",
            "prev_offset",
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_column_key_hash__",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("append").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name"]
    )
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )
    target_data_ = target_data.drop(
        *[
            "prev_name",
            "prev_offset",
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_column_key_hash__",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)
    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.update(6, "value", 106))
    before_change_pd = pd.DataFrame(sample_data.data)
    source_data_change = spark_context.spark.createDataFrame(data_pd)
    source_data_change.write.format("delta").mode("append").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data_ = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name"]
    )
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )
    # checking the rows with source and target old and new columns
    target_data_ = target_data.filter("id<>7").drop(
        *[
            "prev_name",
            "prev_offset",
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_column_key_hash__",
        ]
    )
    assertDataFrameEqual(source_data_, target_data_)

    # checking the rows with source and target old and new columns
    source_data_ = (
        spark_context.spark.createDataFrame(before_change_pd)
        .drop_duplicates(["id", "offset", "name"])
        .filter("id=6")
    )
    target_data_ = (
        target_data.filter("id=6")
        .select("id", "prev_name", "offset")
        .withColumnsRenamed({"prev_name": "name"})
    )

    assertDataFrameEqual(source_data_, target_data_)


def test_stream_delta_source_delta_sink_append_schema_inference(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read in stream mode and writing to delta target with append write operation where input has the schema inference to expand the data
    """
    from datetime import datetime
    import json
    from oneflow.core.utils.spark import from_json

    current_test_folder = (
        f"{setup_teardown}/test_stream_delta_source_delta_sink_append_schema_inference"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=0o777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )

    sample_data = [
        {"id": 1, "ModifiedDate": "2023-02-12", "value": '{"id": 1, "version": 2}'},
        {
            "id": 2,
            "ModifiedDate": datetime.now().strftime("%Y-%m-%d"),
            "value": '{"id": 1, "version": 2}',
        },
    ]
    data_pd = pd.DataFrame(sample_data)
    logger.info(f"Creating the filter feature to test the schema ")
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Generated schema for Root",
        "type": "object",
        "properties": {"id": {"type": "integer"}, "version": {"type": "integer"}},
    }
    with open(f"{current_test_folder}/schema.json", "w") as f:
        f.write(json.dumps(json_schema))

    schema_inference_feature = SchemaInferenceFromRegistry(
        source_column_name="value",
        target_column_name="value_json",
        schema_type="json",
        file_name=f"{current_test_folder}/schema.json",
    )
    input_feature = InputFeatureOptions(
        schema_inference_from_registry=schema_inference_feature
    )

    # source data initialization
    source_data = spark_context.spark.createDataFrame(data_pd)
    # writing the source data
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    logger.info(
        f"Completed the writing to sample delta file data into {current_test_folder}/delta_source"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="stream",
        features=input_feature,
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )
    output_config = SparkOutput(
        name="data",
        path=f"{current_test_folder}/target/",
        sink_type="file",
        write_type="append",
        sink="delta",
    )

    refresh_policy = SparkTaskRefreshPolicy(type="stream")
    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest102")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setRefreshPolicy(refresh_policy)
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data = source_data.withColumn(
        "value_json", from_json("value", file=f"{current_test_folder}/schema.json")
    )
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )
    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)
