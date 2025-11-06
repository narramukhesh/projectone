from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel
from delta import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from oneflow.core.exception.execution import WriteTableConstraintError
from oneflow.core.exception.execution import WriteTypeAttributesError
from oneflow.core.utils.delta import DeltaUtils
from oneflow.core.observability import Logger
from functools import reduce
from copy import deepcopy

logger = Logger.get_logger(__name__)

METADATA_COLUMNS = {
    "__metadata_key_hash__": "__metadata_key_hash__",
    "__metadata_data_hash__": "__metadata_data_hash__",
    "__metadata_valid_to_ts__": "__metadata_valid_to_ts__",
    "__metadata_valid_from_ts__": "__metadata_valid_from_ts__",
    "__metadata_active__": "__metadata_active__",
    "__metadata_insert_ts__": "__metadata_insert_ts__",
    "__metadata_update_ts__": "__metadata_update_ts__",
    "__metadata_column_key_hash__": "__metadata_column_key_hash__",
}


def scd3(
    source_df: DataFrame,
    table_name: str,
    key_attributes: list,
    column_attributes: list,
    options: dict,
    batchId: str = None,
    batchAppName: str = None,
    user_metadata: str = None,
):
    """
    This is Delta Table implementation of the scd type3 pattern

    Parameters
    -----------------
    spark: SparkSession
        This is spark session object
    source_df: DataFrame
        This is source dataframe
    table_name: DataFrame
        This is target table to be merged into
    key_attributes: list
        list of the key attributes which are merged into
    column_attributes: list
        list of column to generate the previous and current column version
    options: dict
        options dictionary to be applied for writing
    batchId: str
        unique batch id to written to target source table
    batchAppName: str
        unique batch app name to be written target source table
    """
    logger.info(
        f"Executing the scd3 write operation for {table_name} with primary key {key_attributes} and column key attributes {column_attributes}"
    )
    if key_attributes is None or column_attributes is None:
        raise WriteTypeAttributesError(
            "Missing Required key attributes and column attributes"
        )
    spark = source_df.sparkSession
    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", user_metadata)
    if options.get("file", None):
        target = DeltaTable.forPath(spark, table_name)
    else:
        target = DeltaTable.forName(spark, table_name)

    local_metadata_columns = METADATA_COLUMNS.copy()
    extra_options = None
    # setting the extra options for customizing the write
    if "extra_options" in options:
        extra_options = options["extra_options"]
        del options["extra_options"]

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.persist(storageLevel=StorageLevel.DISK_ONLY)
    if (extra_options is not None) and ("stage_results" in extra_options):
        if extra_options["stage_results"]:
            source_df = source_df.localCheckpoint()

    # renaming the metadata columns where downstream table can have different names
    if (extra_options is not None) and ("rename_metadata_columns" in extra_options):
        if (extra_options["rename_metadata_columns"] is not None) and isinstance(
            extra_options["rename_metadata_columns"], dict
        ):
            for i in local_metadata_columns:
                local_metadata_columns[i] = extra_options[
                    "rename_metadata_columns"
                ].get(i, local_metadata_columns[i])

    source_columns = source_df.columns
    key_attributes = (
        key_attributes.split(",") if isinstance(key_attributes, str) else key_attributes
    )

    key_attributes_cols = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in key_attributes
    ]
    column_attributes = (
        column_attributes.split(",")
        if isinstance(column_attributes, str)
        else column_attributes
    )
    column_attributes_cols = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in column_attributes
    ]
    data_attributes_cols = [
        i for i in source_columns if i not in key_attributes + column_attributes
    ]
    if "data_attributes" in options:
        if options["data_attributes"] is not None:
            data_attributes_cols = (
                options["data_attributes"].split(",")
                if isinstance(options["data_attributes"], str)
                else options["data_attributes"]
            )
    data_attributes = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in data_attributes_cols
    ]

    source_generate_metadata_columns = {
        f"{local_metadata_columns['__metadata_key_hash__']}": F.md5(
            F.concat_ws("|", *key_attributes_cols)
        ),
        f"{local_metadata_columns['__metadata_data_hash__']}": F.md5(
            F.concat_ws("|", *data_attributes)
        ),
        f"{local_metadata_columns['__metadata_column_key_hash__']}": F.md5(
            F.concat_ws("|", *column_attributes_cols)
        ),
        f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.current_timestamp(),
        f"{local_metadata_columns['__metadata_valid_from_ts__']}": F.current_timestamp(),
    }

    insert_col_mapping = {
        f"target.{key}": f"source.{key}"
        for key in source_columns
        + [
            local_metadata_columns["__metadata_key_hash__"],
            local_metadata_columns["__metadata_data_hash__"],
            local_metadata_columns["__metadata_valid_to_ts__"],
            local_metadata_columns["__metadata_valid_from_ts__"],
            local_metadata_columns["__metadata_column_key_hash__"],
        ]
    }

    update_col_mapping = {
        f"target.{key}": f"source.{key}"
        for key in data_attributes_cols
        + [
            f"{local_metadata_columns['__metadata_valid_to_ts__']}",
            f"{local_metadata_columns['__metadata_data_hash__']}",
        ]
    }

    scd3_merge_condition = f"target.{local_metadata_columns['__metadata_key_hash__']}=source.{local_metadata_columns['__metadata_key_hash__']}"

    if (extra_options is not None) and (
        "generate_record_upsert_columns" in extra_options
    ):
        if extra_options["generate_record_upsert_columns"]:
            source_generate_metadata_columns = {
                **source_generate_metadata_columns,
                **{
                    f"{local_metadata_columns['__metadata_insert_ts__']}": F.current_timestamp(),
                    f"{local_metadata_columns['__metadata_update_ts__']}": F.current_timestamp(),
                },
            }
            update_col_mapping = {
                **update_col_mapping,
                **{
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                },
            }
            insert_col_mapping = {
                **insert_col_mapping,
                **{
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                    f"target.{local_metadata_columns['__metadata_insert_ts__']}": f"source.{local_metadata_columns['__metadata_insert_ts__']}",
                },
            }

    if (extra_options is not None) and ("use_key_attributes_in_merge" in extra_options):
        if extra_options["use_key_attributes_in_merge"]:
            key_attributes_expr_string = " and ".join(
                [f"target.{i}<=>source.{i}" for i in key_attributes]
            )
            scd3_merge_condition = (
                f"{key_attributes_expr_string} and {scd3_merge_condition}"
            )

    if (extra_options is not None) and (
        "use_paritition_column_in_merge" in extra_options
    ):
        if (extra_options["use_paritition_column_in_merge"] is not None) and isinstance(
            extra_options["use_paritition_column_in_merge"], list
        ):
            parition_col_expr_string = " and ".join(
                [
                    f"target.{i}<=>source.{i}"
                    for i in extra_options["use_paritition_column_in_merge"]
                ]
            )
            scd3_merge_condition = (
                f"{parition_col_expr_string} and {scd3_merge_condition}"
            )

    update_change_col_mapping = deepcopy(update_col_mapping)
    for key in column_attributes:
        update_change_col_mapping[f"target.prev_{key}"] = F.when(
            F.col(f"target.{key}") != F.col(f"source.{key}"), F.col(f"target.{key}")
        ).otherwise(F.col(f"target.prev_{key}"))
        update_change_col_mapping[f"target.{key}"] = f"source.{key}"
    update_change_col_mapping[
        f"target.{local_metadata_columns['__metadata_column_key_hash__']}"
    ] = f"source.{local_metadata_columns['__metadata_column_key_hash__']}"

    if (extra_options is not None) and ("change_tracking_columns" in extra_options):
        if isinstance(extra_options["change_tracking_columns"], dict) and (
            extra_options["change_tracking_columns"] is not None
        ):
            if (
                ("target" in extra_options["change_tracking_columns"])
                and ("on" in extra_options["change_tracking_columns"])
                and ("default" in extra_options["change_tracking_columns"])
            ):
                target_col = extra_options["change_tracking_columns"]["target"]
                change_tracking_source = extra_options["change_tracking_columns"]["on"]
                change_tracking_source = (
                    change_tracking_source.split(",")
                    if isinstance(change_tracking_source, str)
                    else change_tracking_source
                )
                change_tracking_source_cols = reduce(
                    lambda a, b: a & b,
                    [
                        F.col(f"target.{i}") != F.col(f"source.{i}")
                        for i in change_tracking_source
                    ],
                )
                change_tracking_condition = F.when(
                    change_tracking_source_cols,
                    F.lit(extra_options["change_tracking_columns"]["default"]),
                ).otherwise(F.col(f"source.{target_col}"))
                update_change_col_mapping[f"target.{target_col}"] = (
                    change_tracking_condition
                )

    source_transform_df = source_df
    if (extra_options is not None) and ("deduplicate_onkeys" in extra_options):
        if extra_options["deduplicate_onkeys"] and len(key_attributes) > 0:
            source_transform_df = source_transform_df.dropDuplicates(key_attributes)

    source_data = source_transform_df.withColumns(
        {f"prev_{key}": F.lit(None) for key in column_attributes}
    ).withColumns(source_generate_metadata_columns)

    ## generated columns
    if (
        (extra_options is not None)
        and ("generated_cols" in extra_options)
        and (extra_options["generated_cols"] is not None)
    ):
        generated_expr = {k: F.expr(v) for k, v in extra_options["generated_cols"]}
        source_data = source_data.withColumns(generated_expr)

        for k in extra_options["generated_cols"]:
            insert_col_mapping[f"target.{k}"] = insert_col_mapping[f"source.{k}"]
            update_col_mapping[f"target.{k}"] = update_col_mapping[f"source.{k}"]

    scd3_output = (
        target.alias("target")
        .merge(
            source_data.alias("source"),
            condition=scd3_merge_condition,
        )
        .whenMatchedUpdate(
            condition=f"target.{local_metadata_columns['__metadata_data_hash__']}<>source.{local_metadata_columns['__metadata_data_hash__']} and target.{local_metadata_columns['__metadata_column_key_hash__']}=source.{local_metadata_columns['__metadata_column_key_hash__']}",
            set=update_col_mapping,
        )
        .whenMatchedUpdate(
            condition=f"target.{local_metadata_columns['__metadata_column_key_hash__']}<>source.{local_metadata_columns['__metadata_column_key_hash__']}",
            set=update_change_col_mapping,
        )
        .whenNotMatchedInsert(values=insert_col_mapping)
    )
    scd3_output.execute()

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df.unpersist()

    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "")

    try:
        if options.get("format", "delta") == "delta":
            table_type = "file" if options.get("file", None) else "table"
            stats = DeltaUtils.delta_max_version_stats(
                spark=spark, tableName=table_name, nameType=table_type
            )
            logger.info(
                f"Completed the scd3 write operation for {table_name} with stats {stats}"
            )
            return stats
    except Exception as e:
        logger.warning(
            f"Error in fetching the output write statistics for scd3 type failed because of issue {e}"
        )
    logger.info(f"Completed the scd3 write operation for {table_name}")


def scd1(
    source_df: DataFrame,
    table_name: str,
    key_attributes: list,
    options: dict,
    batchId: str = None,
    batchAppName: str = None,
    user_metadata: str = None,
):
    """
    This is Delta Table implementation of the scd type1 pattern

    Parameters
    -----------------
    spark: SparkSession
        This is spark session object
    source_df: DataFrame
        This is source dataframe
    table_name: DataFrame
        This is target table to be merged into
    key_attributes: list
        list of the key attributes which are merged into
    column_attributes: list
        list of column to generate the previous and current column version
    options: dict
        options dictionary to be applied for writing
    batchId: str
        unique batch id to written to target source table
    batchAppName: str
        unique batch app name to be written target source table
    """
    logger.info(
        f"Executing the scd1 write operation for {table_name} with primary key {key_attributes}"
    )
    if key_attributes is None:
        raise WriteTypeAttributesError("Missing Required key attributes")
    spark = source_df.sparkSession

    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", user_metadata)
    if options.get("file", None):
        target = DeltaTable.forPath(spark, table_name)
    else:
        target = DeltaTable.forName(spark, table_name)

    local_metadata_columns = METADATA_COLUMNS.copy()
    extra_options = None
    # setting the extra options for customizing the write
    if "extra_options" in options:
        extra_options = options["extra_options"]
        del options["extra_options"]

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.persist(storageLevel=StorageLevel.DISK_ONLY)
    if (extra_options is not None) and ("stage_results" in extra_options):
        if extra_options["stage_results"]:
            source_df = source_df.localCheckpoint()
    # renaming the metadata columns where downstream table can have different names
    if (extra_options is not None) and ("rename_metadata_columns" in extra_options):
        if (extra_options["rename_metadata_columns"] is not None) and isinstance(
            extra_options["rename_metadata_columns"], dict
        ):
            for i in local_metadata_columns:
                local_metadata_columns[i] = extra_options[
                    "rename_metadata_columns"
                ].get(i, local_metadata_columns[i])

    source_columns = source_df.columns
    key_attributes = (
        key_attributes.split(",") if isinstance(key_attributes, str) else key_attributes
    )

    key_attributes_cols = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in key_attributes
    ]

    data_attributes_cols = [i for i in source_columns if i not in key_attributes]
    if "data_attributes" in options:
        if options["data_attributes"] is not None:
            data_attributes_cols = (
                options["data_attributes"].split(",")
                if isinstance(options["data_attributes"], str)
                else options["data_attributes"]
            )
    data_attributes = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in data_attributes_cols
    ]
    source_generate_metadata_columns = {
        f"{local_metadata_columns['__metadata_key_hash__']}": F.md5(
            F.concat_ws("|", *key_attributes_cols)
        ),
        f"{local_metadata_columns['__metadata_data_hash__']}": F.md5(
            F.concat_ws("|", *data_attributes)
        ),
        f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.current_timestamp(),
        f"{local_metadata_columns['__metadata_valid_from_ts__']}": F.current_timestamp(),
    }
    insert_col_mapping = {
        f"target.{key}": f"source.{key}"
        for key in source_columns
        + [
            local_metadata_columns["__metadata_key_hash__"],
            local_metadata_columns["__metadata_data_hash__"],
            local_metadata_columns["__metadata_valid_to_ts__"],
            local_metadata_columns["__metadata_valid_from_ts__"],
        ]
    }
    update_col_mapping = {
        f"target.{key}": f"source.{key}"
        for key in data_attributes_cols
        + [
            f"{local_metadata_columns['__metadata_valid_to_ts__']}",
            f"{local_metadata_columns['__metadata_data_hash__']}",
        ]
    }

    scd1_merge_condition = f"target.{local_metadata_columns['__metadata_key_hash__']}=source.{local_metadata_columns['__metadata_key_hash__']}"
    if (extra_options is not None) and (
        "generate_record_upsert_columns" in extra_options
    ):
        if extra_options["generate_record_upsert_columns"]:
            source_generate_metadata_columns = {
                **source_generate_metadata_columns,
                **{
                    f"{local_metadata_columns['__metadata_insert_ts__']}": F.current_timestamp(),
                    f"{local_metadata_columns['__metadata_update_ts__']}": F.current_timestamp(),
                },
            }
            insert_col_mapping = {
                **insert_col_mapping,
                **{
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                    f"target.{local_metadata_columns['__metadata_insert_ts__']}": f"source.{local_metadata_columns['__metadata_insert_ts__']}",
                },
            }
            update_col_mapping = {
                **update_col_mapping,
                **{
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                },
            }

    if (extra_options is not None) and ("use_key_attributes_in_merge" in extra_options):
        if extra_options["use_key_attributes_in_merge"]:
            key_attributes_expr_string = " and ".join(
                [f"target.{i}<=>source.{i}" for i in key_attributes]
            )
            scd1_merge_condition = (
                f"{key_attributes_expr_string} and {scd1_merge_condition}"
            )

    if (extra_options is not None) and (
        "use_paritition_column_in_merge" in extra_options
    ):
        if (extra_options["use_paritition_column_in_merge"] is not None) and isinstance(
            extra_options["use_paritition_column_in_merge"], list
        ):
            parition_col_expr_string = " and ".join(
                [
                    f"target.{i}<=>source.{i}"
                    for i in extra_options["use_paritition_column_in_merge"]
                ]
            )
            scd1_merge_condition = (
                f"{parition_col_expr_string} and {scd1_merge_condition}"
            )

    source_transform_df = source_df
    if (extra_options is not None) and ("deduplicate_onkeys" in extra_options):
        if extra_options["deduplicate_onkeys"] and len(key_attributes) > 0:
            source_transform_df = source_transform_df.dropDuplicates(key_attributes)

    source_transform_df = source_transform_df.withColumns(
        source_generate_metadata_columns
    )

    ## generated columns
    if (
        (extra_options is not None)
        and ("generated_cols" in extra_options)
        and (extra_options["generated_cols"] is not None)
    ):
        generated_expr = {k: F.expr(v) for k, v in extra_options["generated_cols"]}
        source_transform_df = source_transform_df.withColumns(generated_expr)

        for k in extra_options["generated_cols"]:
            insert_col_mapping[f"target.{k}"] = insert_col_mapping[f"source.{k}"]
            update_col_mapping[f"target.{k}"] = update_col_mapping[f"source.{k}"]

    scd1_output = (
        target.alias("target")
        .merge(
            source_transform_df.alias("source"),
            condition=scd1_merge_condition,
        )
        .whenMatchedUpdate(
            condition=f"target.{local_metadata_columns['__metadata_data_hash__']}<>source.{local_metadata_columns['__metadata_data_hash__']}",
            set=update_col_mapping,
        )
        .whenNotMatchedInsert(values=insert_col_mapping)
    )

    scd1_output.execute()

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df.unpersist()
    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "")

    try:
        if options.get("format", "delta") == "delta":
            table_type = "file" if options.get("file", None) else "table"
            stats = DeltaUtils.delta_max_version_stats(
                spark=spark, tableName=table_name, nameType=table_type
            )
            logger.info(
                f"Completed the scd1 write operation for {table_name} with stats {stats}"
            )
    except Exception as e:
        logger.warning(
            f"Error in fetching the output write statistics for scd1 type failed because of issue {e}"
        )
    logger.info(f"Completed the scd1 write operation for {table_name}")


def scd2(
    source_df: DataFrame,
    table_name: str,
    key_attributes: list,
    options: dict,
    batchId: str = None,
    batchAppName: str = None,
    user_metadata: str = None,
):
    """
    This is Delta Table implementation of the scd type2 pattern

    Parameters
    -----------------
    spark: SparkSession
        This is spark session object
    source_df: DataFrame
        This is source dataframe
    table_name: DataFrame
        This is target table to be merged into
    key_attributes: list
        list of the key attributes which are merged into
    column_attributes: list
        list of column to generate the previous and current column version
    options: dict
        options dictionary to be applied for writing
    batchId: str
        unique batch id to written to target source table
    batchAppName: str
        unique batch app name to be written target source table
    """
    logger.info(
        f"Executing the scd2 write operation for {table_name} with primary key {key_attributes}"
    )
    if key_attributes is None:
        raise WriteTypeAttributesError(
            "Missing Required key attributes and column attributes"
        )
    spark = source_df.sparkSession

    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", user_metadata)
    if options.get("file", None):
        target = DeltaTable.forPath(spark, table_name)
    else:
        target = DeltaTable.forName(spark, table_name)
    local_metadata_columns = METADATA_COLUMNS.copy()
    metadata_active_values = {"Y": "Y", "N": "N"}
    extra_options = None
    exclude_data_columns = []
    # setting the extra options for customizing the write
    if "extra_options" in options:
        extra_options = options["extra_options"]
        del options["extra_options"]
    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.persist(storageLevel=StorageLevel.DISK_ONLY)
    if (extra_options is not None) and ("stage_results" in extra_options):
        if extra_options["stage_results"]:
            source_df = source_df.localCheckpoint()
    # renaming the metadata columns where downstream table can have different names
    if (extra_options is not None) and ("rename_metadata_columns" in extra_options):
        if (extra_options["rename_metadata_columns"] is not None) and isinstance(
            extra_options["rename_metadata_columns"], dict
        ):
            for i in local_metadata_columns:
                local_metadata_columns[i] = extra_options[
                    "rename_metadata_columns"
                ].get(i, local_metadata_columns[i])
    # changing the __metadata_active__ column value mapping as passed
    if (extra_options is not None) and ("active_record_value_mapping" in extra_options):
        if (extra_options["active_record_value_mapping"] is not None) and isinstance(
            extra_options["active_record_value_mapping"], dict
        ):
            for i in metadata_active_values:
                metadata_active_values[i] = extra_options[
                    "active_record_value_mapping"
                ].get(i, metadata_active_values[i])

    # exclude the data column from calculating the data hash
    if (extra_options is not None) and (
        extra_options.get("exclude_data_columns", None) is not None
    ):
        exclude_data_columns = (
            extra_options["exclude_data_columns"].split(",")
            if isinstance(extra_options["exclude_data_columns"], str)
            else extra_options["exclude_data_columns"]
        )

    # history_traking_col name where __metadata_valid_from_ts__ column is populated from this column only single column is valid
    if (extra_options is not None) and (
        extra_options.get("history_tracking_col", None) is not None
    ):
        exclude_data_columns = exclude_data_columns + [
            extra_options["history_tracking_col"]
        ]
    key_attributes = (
        key_attributes.split(",") if isinstance(key_attributes, str) else key_attributes
    )

    key_attributes_cols = [
        F.coalesce(F.col(i).cast("string"), F.lit("")) for i in key_attributes
    ]
    source_columns = source_df.columns
    data_attributes = [
        F.coalesce(F.col(i).cast("string"), F.lit(""))
        for i in source_columns
        if i not in key_attributes + exclude_data_columns
    ]

    merge_query_insert_keys = {
        **{f"target.{k}": f"source.{k}" for k in source_columns},
        **{
            f"target.{local_metadata_columns['__metadata_key_hash__']}": f"source.{local_metadata_columns['__metadata_key_hash__']}",
            f"target.{local_metadata_columns['__metadata_data_hash__']}": f"source.{local_metadata_columns['__metadata_data_hash__']}",
            f"target.{local_metadata_columns['__metadata_valid_to_ts__']}": f"source.{local_metadata_columns['__metadata_valid_to_ts__']}",
            f"target.{local_metadata_columns['__metadata_valid_from_ts__']}": f"source.{local_metadata_columns['__metadata_valid_from_ts__']}",
            f"target.{local_metadata_columns['__metadata_active__']}": f"source.{local_metadata_columns['__metadata_active__']}",
        },
    }
    merge_query_update_keys = {
        f"target.{local_metadata_columns['__metadata_active__']}": F.lit(
            f"{metadata_active_values['N']}"
        ),
        f"target.{local_metadata_columns['__metadata_valid_to_ts__']}": f"source.{local_metadata_columns['__metadata_valid_from_ts__']}",
    }
    source_generate_metadata_columns = {
        f"{local_metadata_columns['__metadata_key_hash__']}": F.md5(
            F.concat_ws("|", *key_attributes_cols)
        ),
        f"{local_metadata_columns['__metadata_data_hash__']}": F.md5(
            F.concat_ws("|", *data_attributes)
        ),
        f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.to_timestamp(
            F.lit("9999-12-31")
        ),
        f"{local_metadata_columns['__metadata_valid_from_ts__']}": F.current_timestamp(),
        f"{local_metadata_columns['__metadata_active__']}": F.lit(
            f"{metadata_active_values['Y']}"
        ),
    }

    source_data_update_condition = [
        F.col(f"target.{local_metadata_columns['__metadata_key_hash__']}")
        == F.col(f"source.{local_metadata_columns['__metadata_key_hash__']}"),
        F.col(f"target.{local_metadata_columns['__metadata_active__']}")
        == F.lit(f"{metadata_active_values['Y']}"),
    ]

    scd2_merge_condition = f"target.{local_metadata_columns['__metadata_key_hash__']}=source.merge_key and target.{local_metadata_columns['__metadata_active__']}='{metadata_active_values['Y']}'"
    # history_traking_col name where __metadata_valid_from_ts__ column is populated from this column only single column is valid
    if (extra_options is not None) and (
        extra_options.get("history_tracking_col", None) is not None
    ):
        source_generate_metadata_columns[
            f"{local_metadata_columns['__metadata_valid_from_ts__']}"
        ] = F.col(extra_options["history_tracking_col"])
    if (extra_options is not None) and (
        "generate_record_upsert_columns" in extra_options
    ):
        if extra_options["generate_record_upsert_columns"]:
            merge_query_insert_keys = {
                **merge_query_insert_keys,
                **{
                    f"target.{local_metadata_columns['__metadata_insert_ts__']}": f"source.{local_metadata_columns['__metadata_insert_ts__']}",
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                },
            }
            source_generate_metadata_columns = {
                **source_generate_metadata_columns,
                **{
                    f"{local_metadata_columns['__metadata_insert_ts__']}": F.current_timestamp(),
                    f"{local_metadata_columns['__metadata_update_ts__']}": F.current_timestamp(),
                },
            }
            merge_query_update_keys = {
                **merge_query_update_keys,
                **{
                    f"target.{local_metadata_columns['__metadata_update_ts__']}": f"source.{local_metadata_columns['__metadata_update_ts__']}",
                },
            }

    if (extra_options is not None) and ("use_key_attributes_in_merge" in extra_options):
        if extra_options["use_key_attributes_in_merge"]:
            key_attributes_expr = [
                F.equal_null(F.col(f"target.{i}"), F.col(f"source.{i}"))
                for i in key_attributes
            ]
            key_attributes_expr_string = " and ".join(
                [f"target.{i}<=>source.{i}" for i in key_attributes]
            )
            source_data_update_condition = (
                key_attributes_expr + source_data_update_condition
            )

            scd2_merge_condition = (
                f"{key_attributes_expr_string} and {scd2_merge_condition}"
            )
    if (extra_options is not None) and (
        "use_paritition_column_in_merge" in extra_options
    ):
        if (extra_options["use_paritition_column_in_merge"] is not None) and isinstance(
            extra_options["use_paritition_column_in_merge"], list
        ):
            parition_col_expr = [
                F.equal_null(F.col(f"target.{i}"), F.col(f"source.{i}"))
                for i in extra_options["use_paritition_column_in_merge"]
            ]
            parition_col_expr_string = " and ".join(
                [
                    f"target.{i}<=>source.{i}"
                    for i in extra_options["use_paritition_column_in_merge"]
                ]
            )
            source_data_update_condition = (
                parition_col_expr + source_data_update_condition
            )
            scd2_merge_condition = (
                f"{parition_col_expr_string} and {scd2_merge_condition}"
            )

    source_transform_df = source_df
    if (extra_options is not None) and (("deduplicate_onkeys" in extra_options)):
        if (extra_options.get("deduplicate_onkeys", False)) and len(key_attributes) > 0:
            source_transform_df = source_transform_df.dropDuplicates(key_attributes)
    source_transform_df = source_transform_df.withColumns(
        source_generate_metadata_columns
    )

    # generate the data hash for the excluded data columns

    excluded_data_source_col_hash = F.md5(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(f"source.{i}").cast("string"), F.lit(""))
                for i in exclude_data_columns
                if i != extra_options.get("history_tracking_col", None)
            ],
        )
    )
    excluded_data_target_col_hash = F.md5(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(f"target.{i}").cast("string"), F.lit(""))
                for i in exclude_data_columns
                if i != extra_options.get("history_tracking_col", None)
            ],
        )
    )

    source_data_update = (
        source_transform_df.alias("source")
        .join(
            target.toDF().alias("target"),
            source_data_update_condition,
            "left",
        )
        .withColumns(
            {
                "src_exc_data": excluded_data_source_col_hash,
                "tgt_exc_data": excluded_data_target_col_hash,
            }
        )
        .withColumn(
            "flag",
            F.when(
                (
                    F.col(f"source.{local_metadata_columns['__metadata_data_hash__']}")
                    == F.col(
                        f"target.{local_metadata_columns['__metadata_data_hash__']}"
                    )
                )
                & (F.col("src_exc_data") != F.col("tgt_exc_data")),
                F.lit("U"),
            )
            .when(
                (
                    F.col(f"source.{local_metadata_columns['__metadata_data_hash__']}")
                    == F.col(
                        f"target.{local_metadata_columns['__metadata_data_hash__']}"
                    )
                ),
                F.lit("D"),
            )
            .when(
                F.col(f"source.{local_metadata_columns['__metadata_data_hash__']}")
                != F.col(f"target.{local_metadata_columns['__metadata_data_hash__']}"),
                F.lit("UI"),
            )
            .otherwise(F.lit("I")),
        )
        .selectExpr(
            "source.*",
            "flag",
        )
    )

    ## default __metadata_valid_from_ts__ values columns
    if (
        (extra_options is not None)
        and ("history_start_tracking_value" in extra_options)
        and (extra_options["history_start_tracking_value_type"] is not None)
    ):
        history_start_tracking_value = F.lit(
            extra_options["history_start_tracking_value"]
        )
        if ("history_start_tracking_value" in extra_options) and (
            extra_options["history_start_tracking_value_type"] is not None
        ):
            history_start_tracking_value = history_start_tracking_value.cast(
                extra_options["history_start_tracking_value_type"]
            )
        source_data_update = source_data_update.withColumn(
            f"{local_metadata_columns['__metadata_valid_from_ts__']}",
            F.when(F.col("flag") == F.lit("I"), history_start_tracking_value).otherwise(
                F.col(f"{local_metadata_columns['__metadata_valid_from_ts__']}")
            ),
        )

    if (
        (extra_options is not None)
        and ("fix_duplicates_by_key" in extra_options)
        and ("history_tracking_col" in extra_options)
        and extra_options["fix_duplicates_by_key"]
        and (extra_options["history_tracking_col"] is not None)
    ):
        order_window = Window.partitionBy(
            f"{local_metadata_columns['__metadata_key_hash__']}"
        ).orderBy(
            F.col(f"{local_metadata_columns['__metadata_valid_from_ts__']}").asc()
        )

        ### Drop Duplicates window to handle the duplicates from the same key_hash and data_hash

        source_data_fix = (
            source_data_update.filter("flag!='D'")
            .withColumns(
                {
                    "dr_rw_data": F.lag(
                        F.col(f"{local_metadata_columns['__metadata_data_hash__']}")
                    ).over(order_window),
                    "dr_rw_key": F.lag(
                        F.col(f"{local_metadata_columns['__metadata_key_hash__']}")
                    ).over(order_window),
                }
            )
            .withColumn(
                "dr_flag",
                F.when(
                    (
                        F.col(f"{local_metadata_columns['__metadata_data_hash__']}")
                        == F.col("dr_rw_data")
                    )
                    & (
                        F.col(f"{local_metadata_columns['__metadata_key_hash__']}")
                        == F.col("dr_rw_key")
                    ),
                    F.lit("R"),
                ).otherwise(F.lit("I")),
            )
            .filter("dr_flag='I'")
            .drop("dr_flag", "dr_rw_data", "dr_rw_key")
        )
        source_data_update = (
            source_data_fix.withColumns(
                {
                    "rnk": F.row_number().over(order_window),
                    f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.lead(
                        F.col(f"{local_metadata_columns['__metadata_valid_from_ts__']}")
                    ).over(order_window),
                }
            )
            .withColumn(
                "flag",
                F.when(
                    (F.col("rnk") == F.lit(1))
                    & (
                        F.col(
                            f"{local_metadata_columns['__metadata_valid_to_ts__']}"
                        ).isNotNull()
                    )
                    & (F.col("flag") == F.lit("UI")),
                    F.lit("UI"),
                )
                .when(
                    (F.col("rnk") > F.lit(1)) & (F.col("flag") == F.lit("UI")),
                    F.lit("I"),
                )
                .otherwise(F.col("flag")),
            )
            .withColumn(
                f"{local_metadata_columns['__metadata_active__']}",
                F.when(
                    F.col(
                        f"{local_metadata_columns['__metadata_valid_to_ts__']}"
                    ).isNull(),
                    F.lit(f"{metadata_active_values['Y']}"),
                ).otherwise(F.lit(f"{metadata_active_values['N']}")),
            )
            .withColumn(
                f"{local_metadata_columns['__metadata_valid_to_ts__']}",
                F.when(
                    F.col(
                        f"{local_metadata_columns['__metadata_valid_to_ts__']}"
                    ).isNull(),
                    F.lit("9999-12-31").cast("timestamp"),
                ).otherwise(
                    F.col(f"{local_metadata_columns['__metadata_valid_to_ts__']}")
                ),
            )
            .drop("rnk")
        )

    source_data_ui = source_data_update.filter("flag='UI' or flag='U'").withColumn(
        "merge_key", F.col(f"{local_metadata_columns['__metadata_key_hash__']}")
    )
    source_data_update_ui = source_data_update.filter(
        "flag='UI' or flag='I'"
    ).withColumn("merge_key", F.lit(None))

    source_data = source_data_ui.unionByName(source_data_update_ui)

    ## generated columns
    if (
        (extra_options is not None)
        and ("generated_cols" in extra_options)
        and (extra_options["generated_cols"] is not None)
    ):
        generated_expr = {k: F.expr(v) for k, v in extra_options["generated_cols"]}
        source_data = source_data.withColumns(generated_expr)

        for k in extra_options["generated_cols"]:
            merge_query_insert_keys[f"target.{k}"] = merge_query_insert_keys[
                f"source.{k}"
            ]

    scd2_output = (
        target.alias("target")
        .merge(source_data.alias("source"), scd2_merge_condition)
        .whenMatchedUpdate(set=merge_query_update_keys)
        .whenNotMatchedInsert(values=merge_query_insert_keys)
    )
    scd2_output.execute()
    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df.unpersist()
    if user_metadata is not None:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "")
    try:
        if options.get("format", "delta") == "delta":
            table_type = "file" if options.get("file", None) else "table"
            stats = DeltaUtils.delta_max_version_stats(
                spark=spark, tableName=table_name, nameType=table_type
            )
            logger.info(
                f"Completed the scd2 write operation for {table_name} with stats {stats}"
            )
    except Exception as e:
        logger.warning(
            f"Error in fetching the output write statistics for scd2 type failed because of issue {e}"
        )
    logger.info(f"Completed the scd2 write operation for {table_name}")


def append(
    source_df: DataFrame,
    table_name: str,
    options: dict,
    batchId: str = None,
    batchAppName: str = None,
    user_metadata: str = None,
):
    logger.info(f"Executing the append write operation for {table_name}")
    local_metadata_columns = METADATA_COLUMNS.copy()
    extra_options = None
    # setting the extra options for customizing the write
    if "extra_options" in options:
        extra_options = options["extra_options"]
        del options["extra_options"]

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.cache()

    # renaming the metadata columns where downstream table can have different names
    if (extra_options is not None) and ("rename_metadata_columns" in extra_options):
        if (extra_options["rename_metadata_columns"] is not None) and isinstance(
            extra_options["rename_metadata_columns"], dict
        ):
            for i in local_metadata_columns:
                local_metadata_columns[i] = extra_options[
                    "rename_metadata_columns"
                ].get(i, local_metadata_columns[i])

    source_generate_metadata_columns = {
        f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.current_timestamp()
    }

    if (extra_options is not None) and (
        "generate_record_upsert_columns" in extra_options
    ):
        if extra_options["generate_record_upsert_columns"]:
            source_generate_metadata_columns = {
                **source_generate_metadata_columns,
                **{
                    f"{local_metadata_columns['__metadata_insert_ts__']}": F.current_timestamp()
                },
            }

    source_df = source_df.withColumns(source_generate_metadata_columns)
    spark = source_df.sparkSession
    if options.get("file", None):
        source_df.write.format(options.get("format", "delta")).options(
            **options
        ).option("userMetadata", user_metadata).mode("append").save(table_name)
    else:
        source_df.write.format(options.get("format", "delta")).options(
            **options
        ).option("userMetadata", user_metadata).mode("append").saveAsTable(table_name)

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.unpersist()

    try:
        if options.get("format", "delta") == "delta":
            table_type = "file" if options.get("file", None) else "table"
            stats = DeltaUtils.delta_max_version_stats(
                spark=spark, tableName=table_name, nameType=table_type
            )
            logger.info(
                f"Completed the append write operation for {table_name} with stats {stats}"
            )
    except Exception as e:
        logger.warning(
            f"Error in fetching the output write statistics for append type failed because of issue {e}"
        )
    logger.info(f"Completed the append write operation for {table_name}")


def overwrite(
    source_df: DataFrame,
    table_name: str,
    options: dict,
    batchId: str = None,
    batchAppName: str = None,
    user_metadata: str = None,
):
    logger.info(f"Executing the overwrite write operation for {table_name}")

    local_metadata_columns = METADATA_COLUMNS.copy()
    extra_options = None
    # setting the extra options for customizing the write
    if "extra_options" in options:
        extra_options = options["extra_options"]
        del options["extra_options"]
    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.cache()
    # renaming the metadata columns where downstream table can have different names
    if (extra_options is not None) and ("rename_metadata_columns" in extra_options):
        if (extra_options["rename_metadata_columns"] is not None) and isinstance(
            extra_options["rename_metadata_columns"], dict
        ):
            for i in local_metadata_columns:
                local_metadata_columns[i] = extra_options[
                    "rename_metadata_columns"
                ].get(i, local_metadata_columns[i])

    source_generate_metadata_columns = {
        f"{local_metadata_columns['__metadata_valid_to_ts__']}": F.current_timestamp()
    }
    if (extra_options is not None) and (
        "generate_record_upsert_columns" in extra_options
    ):
        if extra_options["generate_record_upsert_columns"]:
            source_generate_metadata_columns = {
                **source_generate_metadata_columns,
                **{
                    f"{local_metadata_columns['__metadata_insert_ts__']}": F.current_timestamp()
                },
            }

    source_df = source_df.withColumns(source_generate_metadata_columns)
    spark = source_df.sparkSession
    if options.get("file", None):
        source_df.write.format(options.get("format", "delta")).options(
            **options
        ).option("userMetadata", user_metadata).mode("overwrite").save(table_name)
    else:
        source_df.write.format(options.get("format", "delta")).options(
            **options
        ).option("userMetadata", user_metadata).mode("overwrite").saveAsTable(
            table_name
        )

    if (extra_options is not None) and ("persist_dataset" in extra_options):
        if extra_options["persist_dataset"]:
            source_df = source_df.unpersist()

    try:
        if options.get("format", "delta") == "delta":
            table_type = "file" if options.get("file", None) else "table"
            stats = DeltaUtils.delta_max_version_stats(
                spark=spark, tableName=table_name, nameType=table_type
            )
            logger.info(
                f"Completed the overwrite write operation for {table_name} with stats {stats}"
            )
    except Exception as e:
        logger.warning(
            f"Error in fetching the output write statistics for overwrite type failed because of issue {e}"
        )
    logger.info(f"Completed the overwrite write operation for {table_name}")


def table_constraint(source_df: DataFrame, table_name: str):
    """
    This is method for table constraint checking before writing the sink source

    Parameters
    ----------------
    source_df: DataFrame
        This is a source datafame which will be compared against the table
    table_name: str
        This target table where properties are defined, and compared against the source
    """
    spark = source_df.sparkSession
    target = DeltaTable.forName(spark, table_name)
    target_table_details = target.detail()
    target_table_properties = (
        target_table_details.select("properties").collect()[0].properties
    )
    dependent_tables = target_table_properties.get("table.dependent.tables")
    for table, columns in dependent_tables.items():
        source = DeltaTable.forName(spark, table)
        source_count = source_df.join(source.toDF(), columns, "anti").count()
        if source_count > 0:
            raise WriteTableConstraintError(
                f"Source Dataframe of target table {table_name} has failed table constraint with table {table}"
            )
