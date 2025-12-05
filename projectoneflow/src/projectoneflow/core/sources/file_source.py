from projectoneflow.core.sources import SparkSource
from typing import Optional, Any, Type, List, Union, Dict
from pydantic import Field, model_validator
from projectoneflow.core.types import F, C
from projectoneflow.core.schemas.refresh import TaskRefreshTypes as SparkTaskRefreshTypes
from projectoneflow.core.exception.sources import FileSourceCredentialsValidationError
from projectoneflow.core.schemas.state import ChangeFeatureValue
from projectoneflow.core.schemas.sources import (
    SparkSourceType,
    FileSourceType,
    FileSourceCredentials,
    FileCompression,
)
from projectoneflow.core.schemas.result import ChangeDataCaptureResult
from projectoneflow.core.schemas.features import (
    ChangeDataFeatureType,
    PostTaskExecutionFeature,
)
from projectoneflow.core.exception.sources import (
    FileSparkSourceCDCInitializationError,
    WriteFunctionNotImplementedError,
    SharepointRequestException,
    FileDataCompressionParseError,
    SFTPRequestException,
    NoSourceData,
)
from projectoneflow.core.observability.logging import Logger
from pyspark.sql import SparkSession, DataFrame
from projectoneflow.core.utils import remove_folder, DateUtils
from projectoneflow.core.runtime import Runtime
import msal
from office365.graph_client import GraphClient
import pandas as pd
import paramiko
import tempfile
import uuid
import os
import stat
import re
import fnmatch
from pathlib import Path
from pyspark.sql.pandas.types import _to_corrected_pandas_type
from pyspark.sql.types import _parse_datatype_string, StringType


logger = Logger.get_logger(__name__)


class FileClient:
    """File client to do the operation"""

    @classmethod
    def with_spark_session(cls, spark):
        """This is the method to get the file client"""
        client = cls()
        client.spark = spark

    def delete(self, source, target):
        raise NotImplementedError("File Operation delete is not implemented")

    def get_modified_files_list(self, file_path, start_timestamp, end_timestamp):
        """Method to fetch the modified files in provided file path"""
        raise NotImplementedError(
            "File Source doesn't support to get the modified file between range"
        )

    def listdir(self, path):
        """List the directory contents in the provided path"""
        return os.listdir(path=path)

    def get_files_list(self, files):
        """This method helps to get the list of files"""
        file_list = []
        for f in files:
            if os.path.isfile(f):
                file_list.append(f)
            elif os.path.isdir(f):
                for root, dir, fs in os.walk(f):
                    file_list.extend([os.path.join(root, fi) for fi in fs])

        return file_list

    def get_file_name(self, file):
        """This method helps to get the name of the file"""
        return os.path.basename(file)

    def wildcard_resolution(self, file_path):
        """
        This method is used to do the wild card resolution
        """
        file_path = file_path.split(",")
        final_files = []
        wilcard_pattern = r"(\w+[\*\?][\.]?[\w]*)+"
        wilcard_pattern = r"([\*\?])"
        wildcards = ["*", "?"]
        for fi in file_path:

            path_with_wildcard = re.findall(wilcard_pattern, fi)
            if len(path_with_wildcard) > 0:
                paths = fi.strip("/").split("/")
                matched_files = [""]
                for path in paths:
                    temp_files = []
                    if (wildcards[0] in path) or (wildcards[1] in path):
                        for file in matched_files:
                            try:
                                files = self.listdir(file)
                                files = fnmatch.filter(files, path)
                                temp_files.extend([f"{file}/{f}" for f in files])
                            except Exception:
                                continue
                    else:
                        for file in matched_files:
                            file = f"{file}/{path}"
                            temp_files.append(file)
                    matched_files = temp_files
                final_files.extend(matched_files)
            else:
                final_files.append(fi)

        return final_files

    def read_files(
        self,
        file_path: str,
        extention: str,
        compression: List[FileCompression] = None,
        file_type: FileSourceType = FileSourceType.file,
    ):
        """This is a heler method which reads the file at remote location"""
        logger.debug(
            f"Reading the {extention} files at {file_type.value} location {file_path}"
        )
        remote_files = []
        files = self.wildcard_resolution(file_path=file_path)
        remote_files = self.get_files_list(files)
        # creating the temporary folder for uploading the files
        temporary_files_location = os.path.join(tempfile.gettempdir(), uuid.uuid1().hex)
        temporary_files_location_extracted = os.path.join(
            temporary_files_location, "extracted"
        )
        Runtime().atexit(remove_folder, temporary_files_location)
        if not os.path.exists(temporary_files_location_extracted):
            os.makedirs(temporary_files_location_extracted, mode=777, exist_ok=True)
        for file in remote_files:
            file_name = self.get_file_name(file)
            if (not file_name.endswith(extention)) and (compression is not None):
                compression_mapping = {
                    value: file_name.endswith(value.value) for value in compression
                }
                if not any([r for r in compression_mapping.values()]):
                    raise FileDataCompressionParseError(
                        "Provided compression codec doesn't match with file format, Please check the provided compression algorithm"
                    )
                else:
                    local_file = os.path.join(temporary_files_location, file_name)
                    self.get_file(file, local_file)

                    finalized_compression = [
                        k for k, v in compression_mapping.items() if v
                    ][0]
                    compression_function = FileCompression.get_compression_function(
                        finalized_compression
                    )
                    local_file_folder = os.path.join(
                        temporary_files_location_extracted,
                        file_name.split(f".{finalized_compression}")[0],
                    )
                    compression_function(local_file, local_file_folder)
                    logger.debug(
                        f"Downloaded {file_type.value.capitalize()} file is compressed with {finalized_compression.value} copied into location {local_file_folder}"
                    )
            elif file_name.endswith(extention):
                local_file = os.path.join(temporary_files_location_extracted, file_name)
                self.get_file(file, local_file)
                logger.debug(
                    f"Downloaded {file_type.value.capitalize()} file is copied into location {local_file}"
                )
        return temporary_files_location_extracted

    def read_excel(
        self,
        file_path: str,
        sheet_name: str,
        backend: str = "pandas",
        header: str = 0,
        range: str = None,
        compression: List[FileCompression] = None,
        schema: Dict[str, Any] = None,
    ):

        location = self.read_files(
            file_path=file_path,
            extention=".xlsx",
            compression=compression,
            file_type=FileSourceType(
                self.__class__.__name__.replace("Client", "").lower()
            ),
        )
        if backend == "pandas":
            usecols = None
            skiprows = None
            nrows = None
            if range is not None:
                cols = re.findall("([a-zA-Z]+)", range)
                if len(cols) == 1 and range.endswith(":"):
                    usecols = None
                elif len(cols) <= 2:
                    usecols = ":".join(cols)
                rows = re.findall("([0-9]+)", range)
                if len(rows) > 0:
                    skiprows = int(rows[0])
                if len(rows) == 2:
                    nrows = int(rows[1])
            data = pd.DataFrame()

            for root, dir, files in os.walk(location):
                for file in files:
                    if file.endswith(".xlsx"):
                        file = os.path.join(root, file)
                        temp_data = pd.read_excel(
                            file,
                            sheet_name=sheet_name,
                            header=header,
                            skiprows=skiprows,
                            nrows=nrows,
                            usecols=usecols,
                            dtype=schema,
                        )
                        data = pd.concat([temp_data, data])
            return data

    def read_csv(
        self,
        file_path: str,
        header: int = 0,
        skiprows: int = 0,
        backend: str = "pandas",
        compression: List[FileCompression] = None,
        schema: Dict[str, Any] = None,
    ):

        location = self.read_files(
            file_path=file_path,
            extention=".csv",
            compression=compression,
            file_type=FileSourceType(
                self.__class__.__name__.replace("Client", "").lower()
            ),
        )
        if backend == "pandas":
            data = pd.DataFrame()
            for root, dir, files in os.walk(location):
                for file in files:
                    if file.endswith(".csv"):
                        file = os.path.join(root, file)
                        temp_data = pd.read_csv(
                            file, header=header, skiprows=skiprows, dtype=schema
                        )
                        data = pd.concat([temp_data, data])
            return data


class SharepointClient(FileClient):

    @classmethod
    def with_spark_session(cls, spark):
        raise NotImplementedError("Not implemented the class method")

    @classmethod
    def with_client_secrets(
        cls, tenant_id: str, client_id: str, client_secret: str, site_url: str
    ):
        """
        This method is used to autenticate with sharepoint using service principal credentials

        Parameters
        ------------------
        tenant_id: str
            tenant id to autenticate
        client_id:str
            client/app id for autentication
        client_secret:str
            client secret/client private key for autentication
        site_url:str
            site url to which autentication is provided

        Returns
        ---------------------
        Sharepoint
            returns the sharepoint class object
        """

        def _acquire_token():
            authority_url = "https://login.microsoftonline.com/{0}".format(tenant_id)
            app = msal.ConfidentialClientApplication(
                authority=authority_url,
                client_id=client_id,
                client_credential=client_secret,
            )
            result = app.acquire_token_for_client(
                scopes=["https://graph.microsoft.com/.default"]
            )
            return result

        sharepoint = cls()
        sharepoint.context = GraphClient(_acquire_token)
        sharepoint.site = sharepoint.context.sites.get_by_url(site_url)
        return sharepoint

    def delete(self, src, dst):
        """Deletes the source file as provided"""
        raise NotImplementedError("SFTP delete function is not implemented")

    def listdir(self, path):
        """List the directory contents in the provided path"""
        try:
            items = (
                self.site.drive.root.get_by_path(path.strip("/"))
                .children.get()
                .execute_query()
            )
        except Exception as e:
            raise SharepointRequestException(
                f"Failed while requesting the listing the directory resource details from sharepoint location with error {e}"
            )
        directory_items = []
        for item in items:
            directory_items.append(item.name)
        return directory_items

    def walk(self, path, full_path):
        """Walk through the path of the directory"""
        try:
            drive_items = path.children.get().execute_query()
        except Exception as e:
            raise SharepointRequestException(
                f"Failed while requesting the listing the children resource details from sharepoint location {path.web_url} with error {e}"
            )
        result = []
        for drive_item in drive_items:
            if drive_item.is_folder:
                drive_path = full_path + "/" + drive_item.name
                walk_results = self.walk(drive_item, drive_path)
                result.extend(walk_results)
            else:
                drive_item.full_path = full_path + "/" + drive_item.name
                result.append(drive_item)
        return result

    def get_modified_files_list(self, file_path, start_timestamp, end_timestamp):
        """Method to fetch the modified files in provided file path"""
        files = self.wildcard_resolution(file_path=file_path)
        modified_files_list = []
        for file in files:
            try:
                f = (
                    self.site.drive.root.get_by_path(file.strip("/"))
                    .get()
                    .execute_query()
                )
            except Exception as e:
                raise SharepointRequestException(
                    f"Failed while requesting the resource details from sharepoint location {file} with error {e}"
                )
            if f.is_file and (
                f.last_modified_datetime >= start_timestamp
                and f.last_modified_datetime < end_timestamp
            ):
                modified_files_list.append(f"{file}")

            else:
                for i in self.walk(f, file.strip("/")):
                    if i.is_file and (
                        i.last_modified_datetime >= start_timestamp
                        and i.last_modified_datetime < end_timestamp
                    ):
                        modified_files_list.append(i.full_path)
        return modified_files_list

    def get_files_list(self, files):
        """This method helps to get the list of files"""
        remote_files = []
        for path in files:
            try:
                file = (
                    self.site.drive.root.get_by_path(path.strip("/"))
                    .get()
                    .execute_query()
                )
            except Exception as e:
                raise SharepointRequestException(
                    f"Failed while requesting the resource details from sharepoint location {path} with error {e}"
                )

            if not file.is_file:
                list_files = self.walk(file, path.strip("/"))
                for fi in list_files:
                    if fi.is_file:
                        remote_files.append(fi)
            else:
                remote_files.append(file)
        return remote_files

    def get_file_name(self, file):
        """This method helps to get the name of the file"""
        return file.name

    def get_file(self, source_file, target_file):
        """This method downloads/copy the path from source location to target location"""
        try:
            parent_folder = Path(target_file).parent
            if not os.path.exists(parent_folder.__str__()):
                os.makedirs(parent_folder.__str__(), mode=777, exist_ok=True)

            with open(target_file, "wb") as f:
                source_file.download(f).execute_query()
        except Exception as e:
            raise SharepointRequestException(
                f"Failed while requesting the file from sharepoint location with error {e}"
            )


class SFTPClient(FileClient):
    @classmethod
    def with_spark_session(cls, spark):
        raise NotImplementedError("Not implemented the class method")

    @classmethod
    def with_client_secrets(
        cls,
        client_id: str,
        client_secret: str,
        client_certificate: str,
        site_url: str,
        site_port: int = 22,
    ):
        """
        This method is used to autenticate with SFTP using secure credentials

        Parameters
        ------------------
        client_id:str
            client/app id for autentication
        client_secret:str
            client secret/client password for autentication
        client_certificate:str
            client secret/client private key for autentication
        site_url:str
            site url to which autentication is provided
        site_port:str
            site port to connect the site url autentication is provided

        Returns
        ---------------------
        SFTP
            returns the sftp class object
        """
        sftp = cls()
        logger.debug("Creating the SFTP client connection")
        try:
            sftp.ssh_client = paramiko.SSHClient()
            sftp.ssh_client.load_system_host_keys()
            sftp.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            sftp.ssh_client.connect(
                hostname=site_url,
                username=client_id,
                password=client_secret,
                allow_agent=True,
                key_filename=client_certificate,
                port=site_port,
            )
            sftp.sftp_client = sftp.ssh_client.open_sftp()
        except Exception as e:
            raise SFTPRequestException(
                f"Failed while connecting to sftp server({site_url},{client_id},{client_certificate},{client_secret}), because of the error {e}"
            )
        return sftp

    def listdir(self, path):
        """
        List the directory contents in the provided path

        Parameters
        ------------------
        path: str
            path of the directory to list the files
        """
        items = self.sftp_client.listdir(path)
        return items

    def get_modified_files_list(self, file_path, start_timestamp, end_timestamp):
        """Method to fetch the modified files in provided file path"""
        raise NotImplementedError(
            "SFTP Source doesn't support to get the modified file between range"
        )

    def delete(self, src: str, dst: str):
        """
        This method is used to delete the source path

        Parameters
        ------------------
        src: str
            source path to delete
        dst:str
            In this path it is the temporary argument
        """
        files = self.wildcard_resolution(src)
        for file in files:
            if stat.S_ISDIR(self.sftp_client.stat(file).st_mode):
                files_list = self.sftp_client.listdir(file)
                remote_files = [src + "/" + f for f in files_list]
                for f in remote_files:
                    self.sftp_client.remove(f)
            else:
                self.sftp_client.remove(file)

    def get_files_list(self, files: List[str]) -> List[str]:
        """
        This method helps to get the list of files by recursively going through the files

        Parameters
        --------------
        files: List[str]
            files/folders to list the files from

        Returns
        ----------------
        List[str]
            returns the list of the file path
        """

        remote_files = []
        for file in files:
            try:
                file_stats = self.sftp_client.stat(file)
            except Exception as e:
                raise SFTPRequestException(
                    f"Failed while fetching the stats for SFTP location:{file} because of the error {e}"
                )

            if stat.S_ISDIR(file_stats.st_mode):
                list_files = self.listdir(file)
                remote_files.extend([file + "/" + f for f in list_files])
                logger.debug(
                    f"Provide SFTP location {file} is directory with remote files:{remote_files}"
                )
            else:
                remote_files.append(file)
        return remote_files

    def get_file(self, source_file, target_file):
        """This method downloads/copy the path from source location to target location"""
        try:
            self.sftp_client.get(source_file, target_file)
        except Exception as e:
            raise SFTPClient(
                f"Failed while requesting the file from sftp location {source_file} with error {e}"
            )


class FileSource(SparkSource):
    """This class is implementation of the file based sources"""

    read_supported = ["file"]
    read_extract_supported = ["batch"]
    write_supported = ["stream", "file"]
    write_features_supported = []
    write_type_supported = ["append", "overwrite"]

    class ReadOptions(SparkSource.ReadOptions):
        """This class is file source specific read options"""

        file_source: Optional[FileSourceType] = Field(
            FileSourceType.file, description="schema inference from source data"
        )
        file_source_credentials: Optional[FileSourceCredentials] = Field(
            None, description="sharepoint credentials options"
        )
        file_compression: Optional[List[FileCompression]] = Field(
            None,
            description="Source file compression codec to be used to desearialized the files",
        )

        @model_validator(mode="after")
        def validate(self):
            """This method validates the read options provided"""

            if (
                self.file_source in [FileSourceType.sharepoint, FileSourceType.sftp]
                and self.file_source_credentials is None
            ):
                raise FileSourceCredentialsValidationError(
                    f"Missing file_source_credentials value for file source type {self.file_source.value}"
                )

            return self

    @classmethod
    def get_write_function(cls, write_type: str) -> Type[F]:
        """
        This method returns the write function obj for specific write type

        Parameters
        ------------------
        cls: class
        write_type: str
            This will specify the write type to get the return write object
        """
        from projectoneflow.core.execution.write import append, overwrite

        if write_type == "append":
            return append
        elif write_type == "overwrite":
            return overwrite
        else:
            raise WriteFunctionNotImplementedError(
                f"{write_type} write type function is not implemented"
            )

    @staticmethod
    def resolve_post_task_execution(
        spark: SparkSession,
        path: str,
        source_type: str,
        options: Any,
        operation: PostTaskExecutionFeature,
    ):
        """
        This is a method is a implementation for resolving the change table feature

        Parameters
        ------------------
        spark:SparkSession
            spark Session object to be used in connecting the source
        path:str
            path location for delta input source
        source_type:str
            source type name whether file
        options:Any
            file input sources options

        Returns
        -------------
        Any
            returns options which is used for the calling method
        """
        if options.get("file_source", FileSourceType.file) == FileSourceType.sftp:
            client = SFTPClient.with_client_secrets(
                client_id=options["file_source_credentials"]["client_id"],
                client_secret=options["file_source_credentials"]["client_secret"],
                client_certificate=options["file_source_credentials"][
                    "client_certificate"
                ],
                site_url=options["file_source_credentials"]["site_url"],
                site_port=options["file_source_credentials"]["site_port"],
            )
        elif (
            options.get("file_source", FileSourceType.file) == FileSourceType.sharepoint
        ):
            client = SharepointClient.with_client_secrets(
                tenant_id=options["file_source_credentials"]["tenant_id"],
                client_id=options["file_source_credentials"]["client_id"],
                client_secret=options["file_source_credentials"]["client_secret"],
                site_url=options["file_source_credentials"]["site_url"],
            )
        elif options.get("file_source", FileSourceType.file) == FileSourceType.file:
            client = FileSource.get_file_client(spark)

        exec_function = getattr(client, operation.operation.value)
        Runtime().atexit(exec_function, path, operation.target_path)

    @staticmethod
    def resolve_change_data_feature(
        spark: SparkSession,
        path: str,
        source: str,
        source_type: str,
        refresh_policy_type: SparkTaskRefreshTypes,
        previous_cdc_value: Any,
        cdc: Any,
        options: Any,
    ):
        """
        This is a method is a implementation for resolving the change table feature

        Parameters
        ------------------
        spark:SparkSession
            spark Session object to be used in connecting the source
        path:str
            path location for delta input source
        source:str
            source name of the input source
        source_type:str
            source type name whether file/table
        refresh_policy_type:SparkTaskRefreshTypes
            refresh policy type name for the processing the spark cdc
        previous_cdc_value: Any
            This is the parameter to be processed to the further cdc processing source
        cdc:Any
            cdc configuration for this source
        options:Any
            delta input sources options

        Returns
        -------------
        Any
            returns options which is used for the calling method
        """
        attribute = ""
        start_value = ChangeFeatureValue(
            value=cdc.start_value, value_type=cdc.value_type
        )
        end_value = ChangeFeatureValue(value=cdc.end_value, value_type=cdc.value_type)
        extra_info = None
        filter_expr = None
        try:
            if refresh_policy_type in [
                SparkTaskRefreshTypes.incremental,
                SparkTaskRefreshTypes.backfill,
            ]:
                if refresh_policy_type == SparkTaskRefreshTypes.incremental:
                    previous_value = previous_cdc_value.next_value
                    if (
                        cdc.change_feature_type
                        == ChangeDataFeatureType.file_path_cdc_feed
                    ):
                        if options.file_source == FileSourceType.sharepoint:
                            file_client = SharepointClient.with_client_secrets(
                                tenant_id=options.file_source_credentials.tenant_id,
                                client_id=options.file_source_credentials.client_id,
                                client_secret=options.file_source_credentials.client_secret,
                                site_url=options.file_source_credentials.site_url,
                            )

                        elif (
                            options.get("file_source", FileSourceType.file)
                            == FileSourceType.sftp
                        ):
                            file_client = SFTPClient.with_client_secrets(
                                client_id=options.file_source_credentials.client_id,
                                client_secret=options.file_source_credentials.client_secret,
                                client_certificate=options.file_source_credentials.client_certificate,
                                site_url=options.file_source_credentials.site_url,
                                site_port=options.file_source_credentials.site_port,
                            )
                        else:
                            file_client = FileClient()

                        if (previous_value is None) or (
                            hasattr(previous_value, "value")
                            and (getattr(previous_value, "value") is None)
                        ):
                            start_value = ChangeFeatureValue(
                                value=DateUtils.DEFAULT_START_TIME,
                                value_type="timestamp",
                            )
                        else:
                            start_value = ChangeFeatureValue(
                                value=DateUtils.get_datetime(
                                    previous_value.get_python_value()
                                ),
                                value_type="timestamp",
                            )
                        end_value = ChangeFeatureValue(
                            value=DateUtils.get_time(), value_type="timestamp"
                        )
                        try:
                            modified_file_list = file_client.get_modified_files_list(
                                path,
                                start_value.get_python_value(),
                                end_value.get_python_value(),
                            )
                            extra_info = {
                                "cdc_capture_modified_files": modified_file_list
                            }
                            if len(modified_file_list) == 0:
                                raise NoSourceData(
                                    f"Input Source {source} has no modified files"
                                )
                            logger.debug(
                                f"Source {source} CDC feature fetched the modified files:{modified_file_list}"
                            )
                            path = ",".join(modified_file_list)
                            start_value = ChangeFeatureValue(
                                value=DateUtils.to_timestamp(
                                    start_value.get_python_value()
                                ),
                                value_type="integer",
                            )
                            end_value = ChangeFeatureValue(
                                value=DateUtils.to_timestamp(
                                    end_value.get_python_value()
                                ),
                                value_type="integer",
                            )
                        except NoSourceData:
                            logger.warning(
                                f"Tried to fetch the modified files from source {source} but failed because source doesn't have any new files"
                            )
                            raise NoSourceData(
                                f"Input Source {source} has no modified files"
                            )
                        except NoSourceData:
                            logger.warning(
                                f"Tried to fetch the modified files from source {source} but failed because source doesn't support getting the incremental fetch"
                            )
                        except Exception as e:
                            logger.warning(
                                f"Tried to fetch the modified files from source {source} but failed due to error {e}"
                            )
                    else:
                        if source_type == SparkSourceType.file:
                            input = (
                                spark.read.format(source.value.lower())
                                .options(**options.model_dump())
                                .load(path)
                            )
                        else:
                            raise FileSparkSourceCDCInitializationError(
                                "File Source initialization error because of the unsupported spark source type"
                            )
                        prov_start_value = start_value.model_copy(deep=True)
                        start_value = (
                            ChangeFeatureValue(
                                value=input.selectExpr(
                                    f"min({cdc.attribute})"
                                ).collect()[0][0],
                                value_type=cdc.value_type,
                            )
                            if (
                                previous_value is None
                                and prov_start_value.get_python_value() is None
                            )
                            else (
                                previous_value
                                if previous_value is not None
                                else prov_start_value
                            )
                        )
                        end_value = (
                            ChangeFeatureValue(
                                value=input.selectExpr(
                                    f"max({cdc.attribute})"
                                ).collect()[0][0],
                                value_type=cdc.value_type,
                            )
                            if (
                                previous_value is None
                                and prov_start_value.get_python_value() is None
                            )
                            else (
                                ChangeFeatureValue(
                                    value=input.filter(
                                        f"{cdc.attribute}>{previous_value.get_spark_string_value()}"
                                    )
                                    .selectExpr(f"max({cdc.attribute})")
                                    .collect()[0][0],
                                    value_type=cdc.value_type,
                                )
                                if previous_value is not None
                                else ChangeFeatureValue(
                                    value=input.filter(
                                        f"{cdc.attribute}>={prov_start_value.get_spark_string_value()}"
                                    )
                                    .selectExpr(f"max({cdc.attribute})")
                                    .collect()[0][0],
                                    value_type=cdc.value_type,
                                )
                            )
                        )
                        if (
                            start_value.value is not None
                            and end_value.value is not None
                        ):
                            filter_expr = (
                                f"{cdc.attribute} >= {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                                if previous_value is None
                                else (
                                    f"{cdc.attribute} > {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                                    if start_value.get_python_value()
                                    != end_value.get_python_value()
                                    else f"{cdc.attribute} = {start_value.get_spark_string_value()}"
                                )
                            )
                            attribute = cdc.attribute
                        else:
                            filter_expr = f"1=2"

                else:
                    filter_expr = f"{cdc.attribute} >= {start_value.get_spark_string_value()} and {cdc.attribute} <= {end_value.get_spark_string_value()}"
                    attribute = cdc.attribute
                    start_value = start_value
                    end_value = end_value
        except Exception as e:
            raise FileSparkSourceCDCInitializationError(
                f"File Source initialization error because of the error {e}"
            )

        return ChangeDataCaptureResult(
            attribute=attribute,
            start_value=start_value,
            end_value=end_value,
            extra_info=extra_info,
            filter_expr=filter_expr,
            options=options,
            path=path,
        )


class ParquetSource(FileSource):
    """This class is parquet source implementation"""

    read_supported = ["file"]
    read_extract_supported = ["batch"]
    write_supported = None


class CsvSource(FileSource):
    """This class is a csv source implementation"""

    read_supported = ["file"]
    read_extract_supported = ["batch"]
    write_supported = ["file"]
    write_type_supported = ["append", "overwrite"]

    class ReadOptions(FileSource.ReadOptions):
        """This class is csv source read options"""

        inferSchema: Optional[str] = Field(
            "true", description="schema inference from source data"
        )
        header: Optional[str] = Field("true", description="get the first row as header")
        skiprows: Optional[int] = Field(0, description="no. of rows to skip")

    @classmethod
    def read_batch(
        cls: Type[C],
        spark: SparkSession,
        source: str,
        source_type: SparkSourceType,
        path: str,
        options: ReadOptions,
    ) -> DataFrame:
        """
        This is a static method implemented by the csv specific batch implementation

        Parameters
        ----------------
        spark: SparkSession
            This spark session object to communicate the cluster and do actions
        source: str
            source name like csv, parquet
        source_type:str
            source type to define whether file or table
        path: str
            source path location
        options:ReadOptions
            this is the options to be used for the input reader

        Returns
        -----------
        DataFrame
            returns the dataframe
        """
        schema = options.get("source_schema", None)
        pandas_schema = None
        if schema is not None:
            _spark_schema = _parse_datatype_string(schema)
            pandas_schema = {
                i.name: (
                    _to_corrected_pandas_type(i.dataType)
                    if type(i.dataType) is not StringType
                    else str
                )
                for i in _spark_schema.fields
                if _to_corrected_pandas_type(i.dataType) is not None
                or type(i.dataType) is StringType
            }
        if options.get("file_source", FileSourceType.file) == FileSourceType.sftp:
            sftp = SFTPClient.with_client_secrets(
                client_id=options["file_source_credentials"]["client_id"],
                client_secret=options["file_source_credentials"]["client_secret"],
                client_certificate=options["file_source_credentials"][
                    "client_certificate"
                ],
                site_url=options["file_source_credentials"]["site_url"],
                site_port=options["file_source_credentials"]["site_port"],
            )
            csv_data = sftp.read_csv(
                file_path=path,
                header=0 if options["header"] == "true" else int(options["header"]),
                skiprows=options.get("skiprows", 0),
                compression=options["file_compression"],
                schema=pandas_schema,
            )
            if csv_data.shape[0] == 0:
                raise NoSourceData(f"Input Source {source} has no data")
            if schema is not None:
                df = spark.createDataFrame(csv_data, schema=schema)
            else:
                df = spark.createDataFrame(csv_data)
        else:
            if source_type == SparkSourceType.file:
                df = spark.read.format(source).options(**options)
                if schema is not None:
                    df = df.schema(schema)
                df = df.load(path)
        return df


class ExcelSource(FileSource):
    """This class is a csv source implementation"""

    read_supported = ["file"]
    read_extract_supported = ["batch"]
    write_supported = ["file"]
    write_type_supported = ["append", "overwrite"]

    class ReadOptions(FileSource.ReadOptions):
        """This class is csv source read options"""

        inferSchema: Optional[str] = Field(
            "true", description="schema inference from source data"
        )
        header: Optional[str] = Field(
            "true",
            description="get the first row as header/ specify the row number to consider as header",
        )
        range: Optional[str] = Field(None, description="get the first row as header")
        maxRowsInMemory: Optional[int] = Field(
            None, description="maximum rows in memory to be inserted"
        )
        sheet_name: Union[str, int] = Field(
            ..., description="sheet name from excel to fetch the data"
        )

    @classmethod
    def read_batch(
        cls: Type[C],
        spark: SparkSession,
        source: str,
        source_type: SparkSourceType,
        path: str,
        options: ReadOptions,
    ) -> DataFrame:
        """
        This is a static method implemented by the excel specific batch implementation

        Parameters
        ----------------
        spark: SparkSession
            This spark session object to communicate the cluster and do actions
        source: str
            source name like csv, parquet
        source_type:str
            source type to define whether file or table
        path: str
            source path location
        options:ReadOptions
            this is the options to be used for the input reader

        Returns
        -----------
        DataFrame
            returns the dataframe
        """
        schema = options.get("source_schema", None)
        pandas_schema = None
        if schema is not None:
            _spark_schema = _parse_datatype_string(schema)
            pandas_schema = {
                i.name: (
                    _to_corrected_pandas_type(i.dataType)
                    if type(i.dataType) is not StringType
                    else str
                )
                for i in _spark_schema.fields
                if _to_corrected_pandas_type(i.dataType) is not None
                or type(i.dataType) is StringType
            }

        if options.get("file_source", FileSourceType.file) == FileSourceType.sharepoint:
            sharepoint = SharepointClient.with_client_secrets(
                tenant_id=options["file_source_credentials"]["tenant_id"],
                client_id=options["file_source_credentials"]["client_id"],
                client_secret=options["file_source_credentials"]["client_secret"],
                site_url=options["file_source_credentials"]["site_url"],
            )
            excel_data = sharepoint.read_excel(
                file_path=path,
                sheet_name=options["sheet_name"],
                header=0 if options["header"] == "true" else int(options["header"]),
                range=options["range"],
                compression=options["file_compression"],
                schema=pandas_schema,
            )
            if excel_data.shape[0] == 0:
                raise NoSourceData(f"Input Source {source} has no data")
            if schema is not None:
                df = spark.createDataFrame(excel_data, schema=schema)
            else:
                df = spark.createDataFrame(excel_data)
        elif options.get("file_source", FileSourceType.file) == FileSourceType.sftp:
            sftp = SFTPClient.with_client_secrets(
                client_id=options["file_source_credentials"]["client_id"],
                client_secret=options["file_source_credentials"]["client_secret"],
                client_certificate=options["file_source_credentials"][
                    "client_certificate"
                ],
                site_url=options["file_source_credentials"]["site_url"],
                site_port=options["file_source_credentials"]["site_port"],
            )
            excel_data = sftp.read_excel(
                file_path=path,
                sheet_name=options["sheet_name"],
                header=0 if options["header"] == "true" else int(options["header"]),
                range=options["range"],
                compression=options["file_compression"],
                schema=pandas_schema,
            )
            if excel_data.shape[0] == 0:
                raise NoSourceData(f"Input Source {source} has no data")
            df = spark.createDataFrame(excel_data)
        else:
            logger.warning(
                "Excel Data Source has a dependency on com.crealytics.spark.excel jar file, Please check in dependencies"
            )
            if source_type == SparkSourceType.file:
                df = spark.read.format("com.crealytics.spark.excel").options(**options)
                if schema is not None:
                    df = df.schema(schema)
                df = df.load(path)
        return df
