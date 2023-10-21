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
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContainerClient
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
        self.FLIGHT_RECORDS_CONTAINER = os.getenv("FLIGHT_RECORDS_CONTAINER")
        self.blob_client = self.auth_blob_client()
        self.MACHINE_CPUS = 4
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

        absolute_path = Path.cwd()
        relative_path = "input"
        QARDirIn = absolute_path / relative_path
        self.QARDirIn = str(Path(QARDirIn))

        relative_path = "output"
        OutDirIn = absolute_path / relative_path
        self.OutDirIn = str(Path(OutDirIn))

        relative_path = "scripts"
        ScriptsDirIn = absolute_path / relative_path
        self.ScriptsDirIn = str(Path(ScriptsDirIn))

    def auth_blob_client(self):
        client = BlobServiceClient.from_connection_string(
            self.STORAGE_CONNECTION_STRING
        )
        return client

    def get_runtime(self):
        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.ANALYTICS_CONTAINER, blob="config/runtime.json"
            )

            downloader = blob_client.download_blob(max_concurrency=1, encoding="UTF-8")
            blob_text = downloader.readall()
            runtime = json.loads(blob_text)
            self.icds = runtime["icds"]
            self.package = runtime["package"]
            print("Runtime: ", runtime, flush=True)
            return runtime
        except Exception as e:
            print("Error retrieving runtime config: ", e, flush=True)
            self.rollback()

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
            self.rollback()

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
            self.rollback()

    def read_sb_queue(self, sample):
        _airline = sample["airline"]
        _tail = sample["tail"]
        _package = sample["package"]

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
                                with AutoLockRenewer() as auto_lock_renewer:  # extend lock lease
                                    auto_lock_renewer.register(
                                        receiver, msg, max_lock_renewal_duration=60
                                    )
                                    print(
                                        "Message received, lock lease extended",
                                        flush=True,
                                    )
                                # Read each message
                                data = json.loads(str(msg))
                                airline = data["airline"]
                                tail = data["tail"]
                                file_path = data["file_path"]
                                package = data["package"]
                                if (
                                    airline == _airline
                                    and tail == _tail
                                    and package == _package
                                ):
                                    self.download_blob_to_file(file_path)
                                    receiver.complete_message(msg)
                            except Exception as e:
                                print("Error crawling files: ", e, flush=True)
                                receiver.dead_letter_message(msg)
                                print("Dead-lettering message", flush=True)

                        self.unzip(self.QARDirIn)
                        self.decode(airline, tail)
                        self.upload_blob_files(
                            self.blob_client, self.FLIGHT_RECORDS_CONTAINER
                        )
                    else:
                        break

        # self.restart_program()

    def decode(self, airline, tail):
        # Decode binary
        os.chdir(self.ScriptsDirIn)
        print("Running decode subprocess...", flush=True)

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
        )
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

        # Log output to storage
        self.log_output(airline, tail)

        # Clean input files
        for item in os.scandir(self.QARDirIn):
            if not item.name.startswith("ICDs"):
                # Prints only text file present in My Folder
                os.remove(item)

    def log_output(self, airline, tail):
        # _date = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")
        iso_date = datetime.datetime.now().isoformat()
        now = datetime.datetime.fromisoformat(iso_date)
        date = f"{now.year:02d}" + f"{now.month:02d}"
        path = (
            "logs/"
            + airline
            + "/"
            + tail
            + "/"
            + date
            + "/"
            + iso_date
            + "/"
            + iso_date
            + ".log"
        )

        container_client = self.blob_client.get_container_client(
            container=self.ANALYTICS_CONTAINER
        )
        with open(file=("logfile.log"), mode="rb") as data:
            container_client.upload_blob(name=path, data=data, overwrite=True)
            print("Log uploaded successfully", flush=True)

        # Log all output files
        print("Scanning output dir...", flush=True)
        for item in os.scandir(self.OutDirIn):  # loop through items in output dir
            path = (
                "logs/"
                + airline
                + "/"
                + tail
                + "/"
                + date
                + "/"
                + iso_date
                + "/"
                + item.name
            )
            with open(file=(item), mode="rb") as data:
                container_client.upload_blob(name=path, data=data, overwrite=True)
                print("Uploaded: ", item.name, flush=True)

    def download_blob_to_file(self, file):
        file = file.replace(
            "/blobServices/default/containers/"
            + self.AIRLINE_FLIGHT_DATA_CONTAINER
            + "/blobs/",
            "",
        )
        print("Downloading: ", file, flush=True)
        blob_client = self.blob_client.get_blob_client(
            container=self.AIRLINE_FLIGHT_DATA_CONTAINER, blob=file
        )

        tokens = file.split("/")
        file_name = tokens.pop()

        with open(file=(self.QARDirIn + "/" + file_name), mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
        print("File downloaded successfully", flush=True)

    def upload_blob_files(self, client, container_name):
        container_client = client.get_container_client(container=container_name)
        extension = ".csv"
        for item in os.scandir(self.OutDirIn):  # loop through items in
            print("Scanning output dir...", flush=True)
            if item.name.endswith(extension):  # check for ".csv" extension
                if item.name.startswith("------") or item.name.startswith("raw"):
                    print("Ignore file:", item.name, flush=True)
                else:
                    print("Flight record: ", item.name, flush=True)
                    tokens = item.name.split("_")
                    date = tokens[0]
                    date = "20" + date[0:4]
                    airline = tokens[5]
                    tail_token = tokens[6].split(".")
                    tail = tail_token[0]
                    path = airline + "/" + tail + "/" + date + "/" + item.name
                    with open(file=(item), mode="rb") as data:
                        container_client.upload_blob(
                            name=path, data=data, overwrite=True
                        )
                        print("File uploaded successfully", flush=True)
        for item in os.scandir(self.OutDirIn):
            try:
                shutil.rmtree(item)
            except OSError:
                os.remove(item)

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
                # Read from service bus qar-decode-request for incoming runtime
                sample = self.get_master_file()

                if sample:
                    # Read from service bus qar-decode queue
                    self.read_sb_queue(sample)

                # Get remaining queue msg count
                count = self.get_queue_msg_count()

        except Exception as e:
            print("Error reading from service bus: ", e, flush=True)
            self.rollback()

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
                    received_msgs = receiver.peek_messages(
                        max_message_count=1, max_wait_time=5
                    )
                    if received_msgs:
                        for msg in received_msgs:
                            # ServiceBusReceiver instance is a generator.
                            print("Adding message to batch", flush=True)
                            try:
                                # Read each message
                                data = json.loads(str(msg))

                                package = data["package"]
                                print("Message sample: ", data, flush=True)

                                # Install runtime package
                                self.download_package(package)

                                # Install ICDs
                                self.download_icds()

                                # receiver.complete_message(msg)
                                time.sleep(1)
                                return data

                            except Exception as e:
                                receiver.dead_letter_message(msg)
                                print(
                                    "Error parsing message from sb queue: ",
                                    e,
                                    flush=True,
                                )
                                # self.my_QAR_Decode.terminate()
        except Exception as e:
            print("Error reading from sb queue: ", e, flush=True)

    def unzip(self, qar_dir_in):
        extension = ".zip"

        for item in os.scandir(qar_dir_in):  # loop through items in dir
            if item.name.endswith(extension):  # check for ".zip" extension
                zipdata = zipfile.ZipFile(item)
                zipinfos = zipdata.infolist()
                try:
                    for zipinfo in zipinfos:
                        if (zipinfo.filename).__contains__(".dat") or (
                            zipinfo.filename
                        ).__contains__(".raw"):
                            if (zipinfo.filename).__contains__(".dat"):
                                zipinfo.filename = "raw_" + str(uuid.uuid4()) + ".dat"
                            if (zipinfo.filename).__contains__(".raw"):
                                zipinfo.filename = "raw_" + str(uuid.uuid4()) + ".raw"
                            zipdata.extract(zipinfo, qar_dir_in)
                            zipdata.close()
                            os.remove(item)  # delete zipped file

                except Exception as e:
                    print("Error unzipping: ", e, flush=True)

    def rollback(self):
        try:
            for item in os.scandir(self.OutDirIn):
                # Prints only text file present in My Folder
                os.remove(item)
            for item in os.scandir(self.QARDirIn):
                if not item.name.startswith("ICDs"):
                    # Prints only text file present in My Folder
                    os.remove(item)
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


if __name__ == "__main__":
    decoder = _DecodeRequest()

    schedule.every(5).seconds.do(decoder.read_from_service_bus)

    while True:
        schedule.run_pending()
        time.sleep(1)
