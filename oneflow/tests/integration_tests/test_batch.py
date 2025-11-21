import pandas as pd
import logging
import os
import shutil
from oneflow.core.task.spark import SparkTask
from oneflow.core.schemas.input import SparkInput
from oneflow.core.schemas.output import SparkOutput
from oneflow.core.schemas.sources import (
    WriteOptions,
    WriteExtraOptions,
    WriteExtraOptions,
    ReadOptions,
)
from oneflow.core.schemas.features import (
    CreateDataObjectIfNotExists,
    OutputFeatureOptions,
    InputFeatureOptions,
    ChangeFeature,
    FilterDataFeature,
    SchemaInferenceFromRegistry,
    DropColumnsFeature,
    SelectColumnsFeature,
)
from oneflow.core.schemas.data_objects import Table
from oneflow.core.schemas.execution import SparkExecution
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import json
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual

logging.basicConfig(
    format="%(levelname)s:%(created)f:%(funcName)s:%(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def sample_execution_function(data: DataFrame):
    return data


def test_batch_file_source_delta_sink_append(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of csv read source and writing to delta target with append write operation
    """
    current_test_folder = f"{setup_teardown}/test_batch_file_source_delta_sink_append"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample data into {current_test_folder}/data_test.csv"
    )
    data_pd = pd.DataFrame(sample_data.data)
    data_pd.to_csv(f"{current_test_folder}/data_test.csv", index=False)
    logger.info(
        f"Completed the writing to sample data into {current_test_folder}/data_test.csv"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/data_test.csv",
        source="csv",
        source_type="file",
        source_extract_type="batch",
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("CSVBatchTest101")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )
    source_data = (
        spark_context.spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(f"{current_test_folder}/data_test.csv")
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_batch_delta_source_delta_sink_overwrite(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with overwrite write operation
    """
    current_test_folder = (
        f"{setup_teardown}/test_batch_delta_source_delta_sink_overwrite"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

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
        source_extract_type="batch",
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


def test_batch_delta_source_delta_sink_scd1(setup_teardown, spark_context, sample_data):
    """
    This test used for validation of delta file read and writing to delta target with scd1 write operation
    """
    current_test_folder = f"{setup_teardown}/test_batch_delta_source_delta_sink_scd1"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample delta data into {current_test_folder}/delta_source"
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
        source_extract_type="batch",
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
    source_data.write.format("delta").mode("overwrite").save(
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


def test_batch_delta_source_delta_sink_scd2(setup_teardown, spark_context, sample_data):
    """
    This test used for validation of delta file read and writing to delta target with scd2 write operation
    """
    current_test_folder = f"{setup_teardown}/test_batch_delta_source_delta_sink_scd2"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

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
        source_extract_type="batch",
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
    source_data.write.format("delta").mode("overwrite").save(
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


def test_batch_delta_source_delta_sink_scd3(setup_teardown, spark_context, sample_data):
    """
    This test used for validation of delta file read and writing to delta target with scd3 write operation
    """
    current_test_folder = f"{setup_teardown}/test_batch_delta_source_delta_sink_scd3"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    test_data = sample_data.get_data_with_extra_col(extra=True)
    data_pd = pd.DataFrame(test_data.data)

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
        source_extract_type="batch",
    )

    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(**test_data.create_table(f"{current_test_folder}/target/", "scd3"))
    )
    extra_write_options = WriteExtraOptions(
        change_tracking_columns={"target": "change", "on": "name", "default": 1}
    )
    write_options = WriteOptions(
        key_attributes="id",
        column_attributes="name,offset",
        extra_options=extra_write_options,
    )
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
    data_pd = pd.DataFrame(test_data.set(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name", "change"]
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
    data_pd = pd.DataFrame(test_data.update(6, "value", 106))
    before_change_pd = pd.DataFrame(sample_data.data)
    source_data_change = spark_context.spark.createDataFrame(data_pd)
    source_data_change.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data_ = (
        spark_context.spark.createDataFrame(data_pd)
        .drop_duplicates(["id", "offset", "name", "change"])
        .withColumn(
            "change",
            F.when(
                (F.col("id") == F.lit(6)) & (F.col("name") == F.lit("value")), F.lit(1)
            )
            .otherwise(F.col("change"))
            .cast("long"),
        )
        .select("id", "offset", "name", "change")
    )
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )

    # checking the rows with source and target old and new columns
    target_data_ = target_data.filter("id<>7").select("id", "offset", "name", "change")
    logger.info(f"{target_data_.schema}")
    logger.info(f"{source_data_.schema}")
    assertDataFrameEqual(source_data_, target_data_)

    # checking the rows with source and target old and new columns
    source_data_ = (
        spark_context.spark.createDataFrame(before_change_pd)
        .drop_duplicates(["id", "offset", "name"])
        .filter("id=6")
        .withColumn("change", F.lit(1).cast("long"))
    )
    target_data_ = (
        target_data.filter("id=6")
        .select("id", "prev_name", "offset", "change")
        .withColumnsRenamed({"prev_name": "name"})
    )
    assertDataFrameEqual(source_data_, target_data_)


def test_batch_file_source_delta_sink_append_secret_parsing(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of csv read source and writing to delta target with append write operation which also checks the configuration whether the secrets are parsed correctly or not
    """
    current_test_folder = (
        f"{setup_teardown}/test_batch_file_source_delta_sink_append_secret_parsing"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample data into {current_test_folder}/data_test.csv"
    )
    from datetime import datetime

    sample_data = [
        {"id": 1, "ModifiedDate": "2023-02-12"},
        {"id": 2, "ModifiedDate": datetime.now().strftime("%Y-%m-%d")},
    ]
    data_pd = pd.DataFrame(sample_data)
    data_pd.to_csv(f"{current_test_folder}/data_test.csv", index=False)
    logger.info(
        f"Completed the writing to sample data into {current_test_folder}/data_test.csv"
    )

    logger.info(f"Initializing the spark task to get to be executed")

    logger.info(f"Creating the filter feature to test the secret parsing")
    filter_feature = FilterDataFeature(expression="ModifiedDate < '{{RangeStart}}'")
    input_feature = InputFeatureOptions(filter_data_feature=filter_feature)
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/data_test.csv",
        source="csv",
        source_type="file",
        source_extract_type="batch",
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("CSVBatchTest106")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )
    source_data = (
        spark_context.spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(f"{current_test_folder}/data_test.csv")
    ).filter("ModifiedDate < current_date()")

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_batch_delta_source_delta_sink_append_cdc(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with append write operation
    """
    from oneflow.core.utils.spark import from_json

    current_test_folder = (
        f"{setup_teardown}/test_batch_delta_source_delta_sink_append_cdc"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )

    data_pd = pd.DataFrame(sample_data.cdc_data)

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
    logger.info(f"Creating the cdc feature for the testing the cdc feature")

    logger.info(f"Creating the filter feature to test the schema ")
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Generated schema for Root",
        "type": "object",
        "properties": {"id": {"type": "integer"}, "version": {"type": "integer"}},
    }

    with open(f"{current_test_folder}/schema.json", "w") as f:
        f.write(json.dumps(json_schema))

    filter_feature = ChangeFeature(
        attribute="offset", start_value="101", value_type="integer"
    )
    schema_inference_feature = SchemaInferenceFromRegistry(
        source_column_name="value",
        target_column_name="value_json",
        schema_type="json",
        file_name=f"{current_test_folder}/schema.json",
    )
    input_feature = InputFeatureOptions(
        change_data_feature=filter_feature,
        schema_inference_from_registry=schema_inference_feature,
    )
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="batch",
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest107")
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
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
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

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set_cdc_data(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd).drop_duplicates(
        ["id", "offset", "name", "value"]
    )
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


def test_batch_delta_source_delta_sink_append_all_features(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with append write operation which tests all features
    """
    from oneflow.core.utils.spark import from_json

    current_test_folder = (
        f"{setup_teardown}/test_batch_delta_source_delta_sink_append_all_features"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )

    data_pd = pd.DataFrame(sample_data.cdc_data)

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
    logger.info(f"Creating the cdc feature for the testing the cdc feature")

    logger.info(f"Creating the filter feature to test the schema ")
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Generated schema for Root",
        "type": "object",
        "properties": {"id": {"type": "integer"}, "version": {"type": "integer"}},
    }

    with open(f"{current_test_folder}/schema.json", "w") as f:
        f.write(json.dumps(json_schema))

    cdc_feature = ChangeFeature(attribute="timestamp", value_type="timestamp")
    schema_inference_feature = SchemaInferenceFromRegistry(
        source_column_name="value",
        target_column_name="value_json",
        schema_type="json",
        file_name=f"{current_test_folder}/schema.json",
    )
    filter_feature = FilterDataFeature(expression="offset < 109")
    drop_columns_feature = DropColumnsFeature(columns="name")
    input_feature = InputFeatureOptions(
        change_data_feature=cdc_feature,
        schema_inference_from_registry=schema_inference_feature,
        drop_columns_feature=drop_columns_feature,
        filter_data_feature=filter_feature,
    )
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="batch",
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest107")
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
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data = source_data.withColumn(
        "value_json", from_json("value", file=f"{current_test_folder}/schema.json")
    ).drop("name")
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set_cdc_data(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = (
        spark_context.spark.createDataFrame(data_pd)
        .drop("name")
        .drop_duplicates(["id", "offset", "value", "timestamp"])
    )
    source_data = source_data.withColumn(
        "value_json", from_json("value", file=f"{current_test_folder}/schema.json")
    ).filter("offset < 109")
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_batch_delta_source_delta_sink_scd2_with_metadata_columns_change(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with scd2 write operation with metadata column change
    """
    current_test_folder = f"{setup_teardown}/test_batch_delta_source_delta_sink_scd2_with_metadata_columns_change"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

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
        source_extract_type="batch",
    )
    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(
            **sample_data.create_table(
                f"{current_test_folder}/target/",
                "scd2",
                metadata_column_mapping={
                    "__metadata_valid_from_ts__": "effective_from",
                    "__metadata_valid_to_ts__": "effective_to",
                    "__metadata_key_hash__": "scd_key",
                    "__metadata_data_hash__": "upd_key",
                    "__metadata_active__": "record_status",
                },
            )
        )
    )
    extra_write_options = WriteExtraOptions(
        rename_metadata_columns={
            "__metadata_valid_from_ts__": "effective_from",
            "__metadata_valid_to_ts__": "effective_to",
            "__metadata_key_hash__": "scd_key",
            "__metadata_data_hash__": "upd_key",
            "__metadata_active__": "record_status",
        },
        active_record_value_mapping={"Y": "A", "N": "I"},
    )
    write_options = WriteOptions(
        key_attributes="id,offset", extra_options=extra_write_options
    )
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
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    target_data = spark_context.spark.read.format("delta").load(
        f"{current_test_folder}/target/"
    )
    target_data_ = target_data.filter("record_status='A'").drop(
        *[
            "effective_from",
            "scd_key",
            "upd_key",
            "effective_to",
            "record_status",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("overwrite").save(
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

    target_data_ = target_data.filter("record_status='A'").drop(
        *[
            "effective_from",
            "scd_key",
            "upd_key",
            "effective_to",
            "record_status",
        ]
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    # checking the inactive rows for source and target
    source_data_ = source_data
    target_data_ = target_data.filter("record_status='A'").drop(
        *[
            "effective_from",
            "scd_key",
            "upd_key",
            "effective_to",
            "record_status",
        ]
    )
    assertDataFrameEqual(source_data_, target_data_)


def test_batch_file_source_delta_sink_append_with_source_schema(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of csv read source and writing to delta target with append write operation where modifying the schema to see the dataframe creates same schema
    """
    current_test_folder = (
        f"{setup_teardown}/test_batch_file_source_delta_sink_append_with_source_schema"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample data into {current_test_folder}/data_test.csv"
    )
    data_pd = pd.DataFrame(sample_data.data, dtype=str)
    data_pd.to_csv(f"{current_test_folder}/data_test.csv", index=False)
    logger.info(
        f"Completed the writing to sample data into {current_test_folder}/data_test.csv"
    )

    logger.info(f"Initializing the spark task to get to be executed")
    schema = "id int, name string, offset int"
    read_options = ReadOptions(source_schema=schema)
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/data_test.csv",
        source="csv",
        source_type="file",
        source_extract_type="batch",
        options=read_options,
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("CSVBatchTest101")
        .setMetadataLog(f"{current_test_folder}/pipeline_state/")
        .setSparkconfig("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .setSparkconfig(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .setSparkconfig(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .setExecution(execution_function_config)
        .create()
    )

    spark_task.execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )
    source_data = (
        spark_context.spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .schema(schema)
        .load(f"{current_test_folder}/data_test.csv")
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_batch_delta_source_delta_sink_append_select_columns_features(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with append write operation which tests all features
    """
    from oneflow.core.utils.spark import from_json

    current_test_folder = f"{setup_teardown}/test_batch_delta_source_delta_sink_append_select_columns_features"

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )

    data_pd = pd.DataFrame(sample_data.cdc_data)

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
    logger.info(f"Creating the cdc feature for the testing the cdc feature")

    logger.info(f"Creating the filter feature to test the schema ")
    json_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Generated schema for Root",
        "type": "object",
        "properties": {"id": {"type": "integer"}, "version": {"type": "integer"}},
    }

    with open(f"{current_test_folder}/schema.json", "w") as f:
        f.write(json.dumps(json_schema))

    cdc_feature = ChangeFeature(
        attribute="offset", start_value="101", value_type="integer"
    )
    schema_inference_feature = SchemaInferenceFromRegistry(
        source_column_name="value",
        target_column_name="value_json",
        schema_type="json",
        file_name=f"{current_test_folder}/schema.json",
    )
    filter_feature = FilterDataFeature(expression="offset < 109")
    select_columns_feature = SelectColumnsFeature(columns="id,offset,value,value_json")
    input_feature = InputFeatureOptions(
        change_data_feature=cdc_feature,
        schema_inference_from_registry=schema_inference_feature,
        select_columns_feature=select_columns_feature,
        filter_data_feature=filter_feature,
    )
    input_config = SparkInput(
        name="data",
        path=f"{current_test_folder}/delta_source",
        source="delta",
        source_type="file",
        source_extract_type="batch",
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

    spark_task = (
        SparkTask.builder.setInput(input_config)
        .setOutput(output_config)
        .setName("DeltaBatchTest107")
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
        .setExecution(execution_function_config)
    )

    spark_task.create().execute()
    logger.info(f"Completed the execution task to get to be executed")

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data = source_data.withColumn(
        "value_json", from_json("value", file=f"{current_test_folder}/schema.json")
    ).drop("name", "timestamp")
    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)

    ## inserting the same rows, should have sample to see the after deduplicating the source it should be same as output
    data_pd = pd.DataFrame(sample_data.set_cdc_data(7, "value", 109))
    source_data = spark_context.spark.createDataFrame(data_pd)
    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = (
        spark_context.spark.createDataFrame(data_pd)
        .select("id", "offset", "value")
        .drop_duplicates(["id"])
    )
    source_data = source_data.withColumn(
        "value_json", from_json("value", file=f"{current_test_folder}/schema.json")
    ).filter("offset < 109")

    target_data = (
        spark_context.spark.read.format("delta")
        .load(f"{current_test_folder}/target/")
        .drop(*["__metadata_valid_to_ts__"])
    )

    # checking the source test and target are matching
    assertSchemaEqual(source_data.schema, target_data.schema)
    assertDataFrameEqual(source_data, target_data)


def test_batch_delta_source_delta_sink_scd2_with_history_col(
    setup_teardown, spark_context, sample_data
):
    """
    This test used for validation of delta file read and writing to delta target with scd2 write operation with history tracking col specified by the user
    """
    current_test_folder = (
        f"{setup_teardown}/test_batch_delta_source_delta_sink_scd2_with_history_col"
    )

    if os.path.exists(current_test_folder):
        shutil.rmtree(f"{current_test_folder}", ignore_errors=True)

    os.makedirs(f"{current_test_folder}", mode=777)

    logger.info(
        f"Starting to write the sample deltadata into {current_test_folder}/delta_source"
    )
    data_src = sample_data.get_data_with_extra_col(order_timestamp=True)
    data_pd = pd.DataFrame(data_src.data)

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
        source_extract_type="batch",
    )
    execution_function_config = SparkExecution(
        name="sample_execution_function", type="module", source=f"{__name__}"
    )

    create_table_feature = CreateDataObjectIfNotExists(
        table=Table(**data_src.create_table(f"{current_test_folder}/target/", "scd2"))
    )
    write_options = WriteOptions(
        key_attributes="id,offset",
        extra_options=WriteExtraOptions(
            fix_duplicates_by_key=True, history_tracking_col="timestamp"
        ),
    )
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
    data_pd = pd.DataFrame(
        data_src.bulk_set(
            [
                {
                    "id": 7,
                    "name": "value",
                    "offset": 109,
                    "timestamp": datetime.strptime("2025-01-01", "%Y-%m-%d"),
                },
                {
                    "id": 1,
                    "name": "addr",
                    "offset": 101,
                    "timestamp": datetime.strptime("2025-02-01", "%Y-%m-%d"),
                },
                {
                    "id": 1,
                    "name": "addr2",
                    "offset": 101,
                    "timestamp": datetime.strptime("2025-03-01", "%Y-%m-%d"),
                },
                {
                    "id": 1,
                    "name": "addr2",
                    "offset": 101,
                    "timestamp": datetime.strptime("2025-03-01", "%Y-%m-%d"),
                },
            ]
        )
    )
    source_data = spark_context.spark.createDataFrame(data_pd)

    source_data.write.format("delta").mode("overwrite").save(
        f"{current_test_folder}/delta_source"
    )
    ### executing to insert the rows
    spark_task.create().execute()

    # reading the delta file from target to see the results are matching or not
    source_data = spark_context.spark.createDataFrame(
        pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7],
                "name": ["addr2", "name", "scott", "bob", "fob", "fog", "value"],
                "offset": [101, 102, 103, 104, 105, 106, 109],
                "timestamp": [
                    datetime.strptime("2025-03-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                    datetime.strptime("2025-01-01", "%Y-%m-%d"),
                ],
            }
        )
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
    assert target_data.count() == 9
    assertSchemaEqual(source_data.schema, target_data_.schema)
    assertDataFrameEqual(source_data, target_data_)

    # checking the inactive rows for source and target
    source_data_ = spark_context.spark.createDataFrame(
        pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "oskar",
                    "offset": 101,
                    "timestamp": datetime.strptime("2025-01-01", "%Y-%m-%d"),
                },
                {
                    "id": 1,
                    "name": "addr",
                    "offset": 101,
                    "timestamp": datetime.strptime("2025-02-01", "%Y-%m-%d"),
                },
            ]
        ),
    )
    target_data_ = target_data.filter("__metadata_active__='N'").drop(
        *[
            "__metadata_valid_to_ts__",
            "__metadata_key_hash__",
            "__metadata_data_hash__",
            "__metadata_valid_from_ts__",
            "__metadata_active__",
        ]
    )
    assertDataFrameEqual(source_data_, target_data_)
