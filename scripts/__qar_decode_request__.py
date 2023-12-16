#!/usr/bin/env python
"""
Sample script that uses the QAR_Decode module created using
MATLAB Compiler SDK.

Refer to the MATLAB Compiler SDK documentation for more information.
"""

from __future__ import print_function

import sys
import os
import asyncio
import zipfile, random
import logging
import uuid
import shutil
import importlib
import subprocess
import re
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.data.tables import TableServiceClient
from azure.storage.queue import (
    QueueServiceClient,
    QueueClient,
    QueueMessage,
    TextBase64DecodePolicy,
)
from azure.servicebus import ServiceBusClient, AutoLockRenewer
from azure.servicebus.management import ServiceBusAdministrationClient

import io
from dotenv import load_dotenv
import os
import json
import schedule
import time
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


class _DecodeRequest:
    def __init__(self):
        load_dotenv()
        self.STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
        self.AIRLINE_FLIGHT_DATA_CONTAINER = os.getenv("AIRLINE_FLIGHT_DATA_CONTAINER")
        self.ANALYTICS_CONTAINER = os.getenv("ANALYTICS_CONTAINER")
        self.icds = None
        self.package = None
        self.QAR_Decode = None
        self.my_QAR_Decode = None
        self.run_date = None
        self.FLIGHT_RECORDS_CONTAINER = os.getenv("FLIGHT_RECORDS_CONTAINER")
        self.blob_client = self.auth_blob_client()
        self.table_client = self.auth_table_client()
        self.MACHINE_CPUS = os.cpu_count()
        self.get_runtime()
        # Service Bus queue variables
        self.QUEUE_NAMESPACE_CONN = os.getenv("QUEUE_NAMESPACE_CONN")
        self.QAR_DECODE_REQUEST_QUEUE = os.getenv("QAR_DECODE_REQUEST_QUEUE")
        # Service Bus topic variables
        self.TOPIC_NAMESPACE_CONN = os.getenv("TOPIC_NAMESPACE_CONN")
        self.QAR_DECODE_REQUEST_TOPIC = os.getenv("QAR_DECODE_REQUEST_TOPIC")
        self.QAR_DECODE_REQUEST_TOPIC_SUBSCRIPTION = os.getenv(
            "QAR_DECODE_REQUEST_TOPIC_SUBSCRIPTION"
        )
        self.root = Path.cwd()
        self.create_dirs()

    def create_dirs(self):
        absolute_path = self.root
        relative_path = "input"
        QARDirIn = absolute_path / relative_path
        QARDirIn.mkdir(mode=777, parents=True, exist_ok=True)
        self.QARDirIn = str(Path(QARDirIn))

        relative_path = "input/ICDs"
        ICDsIn = absolute_path / relative_path
        ICDsIn.mkdir(mode=777, parents=True, exist_ok=True)
        self.ICDsIn = str(Path(ICDsIn))

        relative_path = "output"
        OutDirIn = absolute_path / relative_path
        OutDirIn.mkdir(mode=777, parents=True, exist_ok=True)
        self.OutDirIn = str(Path(OutDirIn))

        relative_path = "scripts"
        ScriptsDirIn = absolute_path / relative_path
        self.ScriptsDirIn = str(Path(ScriptsDirIn))

    def auth_blob_client(self):
        client = BlobServiceClient.from_connection_string(
            self.STORAGE_CONNECTION_STRING
        )
        return client

    def auth_table_client(self):
        client = TableServiceClient.from_connection_string(
            self.STORAGE_CONNECTION_STRING
        )
        return client

    def get_runtime(self):
        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.ANALYTICS_CONTAINER, blob="config/qar-decode/config.json"
            )

            downloader = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
            blob_text = downloader.readall()
            runtime = json.loads(blob_text)
            # self.icds = runtime["icds"]
            self.package = runtime["package"]
            print("Runtime: ", runtime, flush=True)
            return runtime
        except Exception as e:
            print("Error retrieving runtime config: ", e, flush=True)

    def download_icds(self):
        try:
            client = self.blob_client.get_container_client(
                container=self.ANALYTICS_CONTAINER
            )
            blob_list = client.list_blobs(name_starts_with="ICDs/")
            for blob in blob_list:
                with open(
                    file=(self.QARDirIn + "/" + blob.name), mode="wb"
                ) as sample_blob:
                    download_stream = client.download_blob(blob)
                    sample_blob.write(download_stream.readall())

            print("ICDs retrieved", flush=True)
        except Exception as e:
            print("Error retrieving ICDs: ", e, flush=True)

    def download_package(self, package):
        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.ANALYTICS_CONTAINER, blob=package
            )

            with open(
                file=(self.ScriptsDirIn + "/QAR_Decode.zip"), mode="wb"
            ) as sample_blob:
                download_stream = blob_client.download_blob()
                sample_blob.write(download_stream.readall())
            print("Package retrieved", flush=True)
            zip_ref = zipfile.ZipFile(
                self.ScriptsDirIn + "/QAR_Decode.zip"
            )  # create zipfile object
            zip_ref.extractall(self.ScriptsDirIn)  # extract file to dir
            time.sleep(2)
            zip_ref.close()  # close file
            os.remove(self.ScriptsDirIn + "/QAR_Decode.zip")  # delete zipped file
            self.install_package()
        except Exception as e:
            print("Error retrieving package: ", e, flush=True)

    def read_sb_queue(self, sample):
        _airline = sample.get("Airline")
        _tail = sample.get("Tail")
        _package = sample.get("Package")
        _overwrite = sample.get("Overwrite")

        # Download and decode matching files
        while True:
            with ServiceBusClient.from_connection_string(
                self.QUEUE_NAMESPACE_CONN
            ) as client:
                # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
                # Default is None; to receive forever.
                # Get message sample
                with client.get_queue_receiver(
                    self.QAR_DECODE_REQUEST_QUEUE
                ) as receiver:
                    received_msgs = receiver.receive_messages(
                        max_message_count=self.MACHINE_CPUS, max_wait_time=5
                    )
                    if received_msgs:
                        for msg in received_msgs:
                            # ServiceBusReceiver instance is a generator.
                            try:
                                # Read each message
                                data = json.loads(str(msg))
                                airline = data.get("Airline")
                                tail = data.get("Tail")
                                file_path = data.get("FilePath")
                                package = data.get("Package")
                                overwrite = data.get("Overwrite")
                                if (
                                    airline == _airline
                                    and tail == _tail
                                    and package == _package
                                    and overwrite == _overwrite
                                ):
                                    print(
                                        "Message matches sample",
                                        flush=True,
                                    )
                                    """
                                    with AutoLockRenewer() as auto_lock_renewer:  # extend lock lease
                                        auto_lock_renewer.register(
                                            receiver, msg, max_lock_renewal_duration=300
                                        )
                                        print(
                                            "Message received, lock lease extended",
                                            flush=True,
                                        )
                                    """
                                    receiver.complete_message(msg)
                                    self.download_blob_to_file(file_path)
                                else:
                                    print(
                                        "Message doesn't match sample, breaking loop... ",
                                        flush=True,
                                    )
                                    # continue
                            except Exception as e:
                                print("Error processing message:", e, flush=True)
                                try:
                                    receiver.dead_letter_message(
                                        msg, e, "Error parsing message from sb queue"
                                    )
                                    print("Dead-lettering message", flush=True)
                                except Exception as e:
                                    print(
                                        "Could not dead-letter message, lock expired",
                                        flush=True,
                                    )

                        self.unzip(self.QARDirIn)
                        self.decode(airline, tail)

                        # Log output to storage
                        self.upload_output(airline, tail)
                        # time.sleep(30)
                    else:
                        print("Didn't receive any messages...", flush=True)
                        time.sleep(30)
                        return
        # self.restart_program()

    def decode(self, airline, tail):
        # Decode binary
        os.chdir(self.ScriptsDirIn)

        count = 0
        for path in os.scandir(self.ScriptsDirIn):
            if path.is_dir():
                count += 1

        if count > 1:
            print("Running decode subprocess...", flush=True)
            try:
                response = subprocess.run(
                    [
                        "python",
                        "__run__.py",
                        f"{self.QARDirIn}",
                        f"{self.OutDirIn}",
                        f"{airline}",
                        f"{tail}",
                    ],
                    capture_output=True,
                    timeout=10800,
                )
            except subprocess.TimeoutExpired:
                print("Timeout expired. Subprocess execution was terminated.")
            except Exception as e:
                print("Error. Subprocess execution was terminated.", e)

            output = response.stdout.decode()
            print("Subprocess finished with code: ", response.returncode, flush=True)
            print(output, flush=True)

            LOG_FORMAT = "%(asctime)s:%(levelname)s ==> %(message)s"
            logging.basicConfig(
                level=logging.WARNING,
                filename="logfile.log",
                filemode="w",
                format=LOG_FORMAT,
                force=True,
            )
            logging.warning(output)

    def log_to_processed(self, blob):
        try:
            FILE_NAME = blob
            PARTITION_KEY = "1"

            my_entity = {
                "PartitionKey": PARTITION_KEY,
                "RowKey": f"{uuid.uuid4()}",
                "FileName": FILE_NAME,
            }

            table_client = self.table_client.create_table_if_not_exists(
                table_name="AirlineFlightDataProcessed"
            )

            entity = table_client.create_entity(entity=my_entity)
            print(
                "Inserted into AirlineFlightDataProcessed =>",
                entity,
                flush=True,
            )
        except Exception as e:
            print("Error inserting into AirlineFlightDataProcessed =>", e, flush=True)

    def download_blob_to_file(self, path):
        tokens = path.split("/")
        self.airline = tokens[1]
        self.tail = tokens[2]
        file_name = tokens.pop()

        blob_client = self.blob_client.get_blob_client(
            container=self.AIRLINE_FLIGHT_DATA_CONTAINER, blob=path
        )

        relative_path = str(file_name).replace(".zip", "")
        _dir_in = Path(self.QARDirIn) / relative_path
        _dir_in.mkdir(mode=777, parents=True, exist_ok=True)
        dir_in = str(Path(_dir_in))

        url = self.winapi_path(dir_in + "/" + file_name)

        print("Downloading =>", file_name, flush=True)

        with open(file=url, mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
        print("File downloaded successfully", flush=True)

        if str(url).endswith(".zip"):
            self.unzip(dir_in)

        self.log_to_processed(path)

    def upload_output(self, airline, tail):
        try:
            now = datetime.datetime.fromisoformat(self.run_date)
            date = f"{now.year:02d}" + f"{now.month:02d}"

            container_client = self.blob_client.get_container_client(
                container=self.ANALYTICS_CONTAINER
            )
            # Log all output files
            dir_in = self.winapi_path(self.QARDirIn)
            for parent in os.scandir(dir_in):  # loop through items in output dir
                print("Scanning input dir...", flush=True)
                tails_path_log = f"logs/qar-decode-request/{airline}/tails/{tail}/{date}/{self.run_date}/{parent.name}/{self.run_date}.log"
                tails_path_run_status = f"logs/qar-decode-request/{airline}/tails/{tail}/{date}/{self.run_date}/{parent.name}/runstatus.json"
                date_path_log = f"logs/qar-decode-request/{airline}/run-date/{self.run_date}/{tail}/{parent.name}/{self.run_date}.log"
                date_path_run_status = f"logs/qar-decode-request/{airline}/run-date/{self.run_date}/{tail}/{parent.name}/runstatus.json"

                if parent.name != "ICDs":
                    # Upload log file
                    with open(file=("logfile.log"), mode="rb") as data:
                        container_client.upload_blob(
                            name=tails_path_log, data=data, overwrite=True
                        )
                    with open(file=("logfile.log"), mode="rb") as data:
                        container_client.upload_blob(
                            name=date_path_log, data=data, overwrite=True
                        )
                    print("Log uploaded successfully", flush=True)

                    # Upload run status
                    try:
                        with open(
                            file=(f"{self.OutDirIn}/runstatus.json"), mode="rb"
                        ) as data:
                            container_client.upload_blob(
                                name=tails_path_run_status, data=data, overwrite=True
                            )
                        with open(
                            file=(f"{self.OutDirIn}/runstatus.json"), mode="rb"
                        ) as data:
                            container_client.upload_blob(
                                name=date_path_run_status, data=data, overwrite=True
                            )
                        print("Run status uploaded successfully", flush=True)
                        os.remove(f"{self.OutDirIn}/runstatus.json")
                    except Exception as e:
                        print("Run status file does not exist", flush=True)

                    dir_in = self.winapi_path(f"{self.QARDirIn}/{parent.name}")
                    for item in os.scandir(dir_in):
                        if item.name.endswith(".csv"):
                            tails_path = f"logs/qar-decode-request/{airline}/tails/{tail}/{date}/{self.run_date}/{parent.name}/{item.name}"
                            date_path = f"logs/qar-decode-request/{airline}/run-date/{self.run_date}/{tail}/{parent.name}/{item.name}"

                            # Upload output files
                            with open(file=(item), mode="rb") as data:
                                container_client.upload_blob(
                                    name=tails_path, data=data, overwrite=True
                                )
                            with open(file=(item), mode="rb") as data:
                                container_client.upload_blob(
                                    name=date_path, data=data, overwrite=True
                                )
                            print("Uploaded to logs: ", item.name, flush=True)

                            # Upload flight record
                            self.upload_flight_record(
                                self.blob_client, self.FLIGHT_RECORDS_CONTAINER, item
                            )
                    # Remove item
                    shutil.rmtree(parent)
        except Exception as e:
            print("Error uploading output files: ", e, flush=True)

    def upload_flight_record(self, client, container_name, blob):
        container_client = client.get_container_client(container=container_name)
        extension = ".csv"
        print("Scanning dir...", flush=True)

        pattern = r"^(\d){6}"

        if blob.name.endswith(extension):  # check for ".csv" extension
            if re.match(pattern, blob.name):
                try:
                    tokens = blob.name.split("_")
                    date = tokens[0]
                    time = tokens[1]
                    flight_id = tokens[2] if len(tokens[2]) else "----"
                    origin = tokens[3] if len(tokens[3]) else "----"
                    dest = tokens[4] if len(tokens[4]) else "----"
                    airline = tokens[5] if len(tokens[5]) == 3 else "unknown"
                    tail_token = tokens[6].split(".")
                    tail = tail_token[0]
                    parent = f"{airline}_{tail.upper()}_{flight_id}_{origin}_{dest}_20{date[0:6]}_{time}Z_----"
                    path = f"{airline}/{tail}/20{date[0:4]}/{parent}/{blob.name}"

                    with open(file=(blob), mode="rb") as data:
                        container_client.upload_blob(
                            name=path, data=data, overwrite=True
                        )
                        print("Uploaded flight record: ", blob.name, flush=True)

                except Exception as e:
                    try:
                        path = f"unknown/{blob.name}"
                        with open(file=(blob), mode="rb") as data:
                            container_client.upload_blob(
                                name=path, data=data, overwrite=True
                            )
                        print("Uploaded as unknown: ", blob.name, flush=True)

                    except Exception as e:
                        print(
                            "Error parsing flight record name: ",
                            blob.name,
                            e,
                            flush=True,
                        )
            else:
                print("Ignore flight record:", blob.name, flush=True)

    def get_queue_msg_count(self):
        with ServiceBusAdministrationClient.from_connection_string(
            self.QUEUE_NAMESPACE_CONN
        ) as client:
            count = client.get_queue_runtime_properties(
                self.QAR_DECODE_REQUEST_QUEUE
            ).active_message_count
        return count

    def read_from_service_bus(self):
        print("Listening...", flush=True)
        try:
            count = self.get_queue_msg_count()

            print("Message count: ", count, flush=True)

            while count > 0:
                self.run_date = datetime.datetime.utcnow().isoformat()

                # clean start
                self.clean()
                self.create_dirs()

                # Read from service bus qar-decode-request for incoming runtime
                sample = self.get_master_file()

                if sample:
                    # Read from service bus qar-decode queue
                    self.read_sb_queue(sample)

                # Get remaining queue msg count
                count = self.get_queue_msg_count()

        except Exception as e:
            print("Error reading from service bus: ", e, flush=True)

    def get_master_file(self):
        print("Reading qar-decode-request queue...", flush=True)
        try:
            with ServiceBusClient.from_connection_string(
                self.QUEUE_NAMESPACE_CONN
            ) as client:
                # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
                # Default is None; to receive forever.
                # Get message sample
                with client.get_queue_receiver(
                    self.QAR_DECODE_REQUEST_QUEUE
                ) as receiver:
                    received_msgs = receiver.peek_messages(max_message_count=1)
                    if received_msgs:
                        for msg in received_msgs:
                            # ServiceBusReceiver instance is a generator.
                            print("Got sample message", flush=True)
                            try:
                                # Read each message
                                data = json.loads(str(msg))

                                package = data.get("Package")
                                print("Message sample: ", data, flush=True)

                                # Install runtime package
                                self.download_package(package)

                                # Install ICDs
                                self.download_icds()

                                # receiver.complete_message(msg)
                                time.sleep(1)
                                return data

                            except Exception as e:
                                receiver.dead_letter_message(
                                    msg, e, "Error parsing message from sb queue"
                                )
                                print(
                                    "Error parsing message from sb queue: ",
                                    e,
                                    flush=True,
                                )
        except Exception as e:
            print("Error reading from sb queue: ", e, flush=True)

    def unzip(self, qar_dir_in):
        extension = ".zip"
        for item in os.scandir(qar_dir_in):  # loop through items in dir
            print("Found zip entry: ", item.name, flush=True)
            if item.name.endswith(extension):  # check for ".zip" extension
                zipdata = zipfile.ZipFile(item)
                zipinfos = zipdata.infolist()
                try:
                    for zipinfo in zipinfos:
                        if (
                            (zipinfo.filename).lower().__contains__(".dat")
                            or (zipinfo.filename).lower().__contains__(".raw")
                            or (zipinfo.filename).endswith("-CPL")
                        ):
                            print("Unzipping =>", zipinfo.filename, flush=True)
                            zipdata.extract(zipinfo, qar_dir_in)
                            print("File unzipped", flush=True)
                        else:
                            print("Unrecognized file: ", zipinfo.filename, flush=True)
                    zipdata.close()
                    os.remove(item)  # delete zipped file
                except Exception as e:
                    print("Error unzipping: ", e, flush=True)
                    zipdata.close()
                    shutil.rmtree(qar_dir_in)  # delete zipped file

    def clean(self):
        try:
            shutil.rmtree(self.OutDirIn)
            shutil.rmtree(self.QARDirIn)
        except Exception as e:
            print("Error cleaning: ", e, flush=True)

    def install_package(self):
        try:
            os.chdir(self.ScriptsDirIn + "/for_redistribution_files_only")
            process = subprocess.Popen(["python", "setup.py", "install"])
            process.wait()

            print("Package installed", flush=True)
        except Exception as e:
            print("Error installing package: ", e, flush=True)

    def restart_program(self):
        """Restarts the current program.
        Note: this function does not return. Any cleanup action (like
        saving data) must be done before calling this function."""
        print("Reinitializing with default parameters... ", flush=True)
        subprocess.call(
            ["python", "C://app/scripts/__qar_decode_request__.py"] + sys.argv[1:]
        )

    def winapi_path(self, dos_path, encoding=None):
        if not isinstance(dos_path, str) and encoding is not None:
            dos_path = dos_path.decode(encoding)
        path = os.path.abspath(dos_path)
        if path.startswith("\\\\"):
            return "\\\\?\\UNC\\" + path[2:]
        return "\\\\?\\" + path


if __name__ == "__main__":
    decoder = _DecodeRequest()

    schedule.every(5).seconds.do(decoder.read_from_service_bus)

    while True:
        schedule.run_pending()
        time.sleep(1)
