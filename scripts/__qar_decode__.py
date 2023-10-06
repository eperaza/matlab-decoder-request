#!/usr/bin/env python
"""
Sample script that uses the QAR_Decode module created using
MATLAB Compiler SDK.

Refer to the MATLAB Compiler SDK documentation for more information.
"""

from __future__ import print_function

# import QAR_Decode_Parallel
# import QAR_Decode
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

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import (
    QueueServiceClient,
    QueueClient,
    QueueMessage,
    TextBase64DecodePolicy,
)
from azure.servicebus import ServiceBusClient
from azure.servicebus.management import ServiceBusAdministrationClient

import io
from dotenv import load_dotenv
import os
import json
import schedule
import time


class Decoder:
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
        self.QAR_DECODE_QUEUE = os.getenv("QAR_DECODE_QUEUE")
        self.blob_client = self.auth_blob_client()
        self.queue_client = self.auth_queue_client()
        self.get_runtime()

        self.QUEUE_NAMESPACE_CONN = "Endpoint=sb://sbns-tspservices-test.servicebus.windows.net/;SharedAccessKeyName=tsp-services;SharedAccessKey=jwyDkYcjjmBRn9WuVi0P+AcToxDTynjL4+ASbJjVHIM=;EntityPath=sbq-qar-decode"
        self.TOPIC_NAMESPACE_CONN = "Endpoint=sb://sbns-tspservices-test.servicebus.windows.net/;SharedAccessKeyName=tsp-services;SharedAccessKey=qzbzXVit/Ts/y/DBA95nVKFS8DW32oD+p+ASbLkgXEU=;EntityPath=sbt-qar-decode"
        self.QAR_DECODE_QUEUE = "sbq-qar-decode"
        self.QAR_DECODE_TOPIC = "sbt-qar-decode"
        self.TOPIC_SUBSCRIPTION = "sbts-runtime-override"
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

        logging.basicConfig()
        schedule_logger = logging.getLogger("schedule")
        schedule_logger.setLevel(level=logging.DEBUG)

    def auth_blob_client(self):
        client = BlobServiceClient.from_connection_string(
            self.STORAGE_CONNECTION_STRING
        )
        return client

    def auth_queue_client(self):
        # Setup Base64 encoding and decoding functions
        client = QueueClient.from_connection_string(
            self.STORAGE_CONNECTION_STRING,
            self.QAR_DECODE_QUEUE,
            message_decode_policy=TextBase64DecodePolicy(),
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
            print("Runtime: ", runtime)
            return runtime
        except Exception as e:
            print("Error retrieving runtime config: ", e, flush=True)
            self.rollback()

    def download_icds(self):
        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.ANALYTICS_CONTAINER, blob=self.icds
            )
            with open(file=(self.QARDirIn + "/ICDs.zip"), mode="wb") as sample_blob:
                download_stream = blob_client.download_blob()
                sample_blob.write(download_stream.readall())
            print("ICDs retrieved", flush=True)
            zip_ref = zipfile.ZipFile(
                self.QARDirIn + "/ICDs.zip"
            )  # create zipfile object
            # print(zip_ref.filename)
            zip_ref.extractall(self.QARDirIn)  # extract file to dir
            zip_ref.close()  # close file
            os.remove(self.QARDirIn + "/ICDs.zip")  # delete zipped file
        except Exception as e:
            print("Error retrieving ICDs: ", e, flush=True)
            self.rollback()

    def download_package(self):
        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.ANALYTICS_CONTAINER, blob=self.package
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

    def decode(self, airline, tail):
        # Decode binary
        self.my_QAR_Decode.QAR_Decode(
            self.QARDirIn, self.OutDirIn, airline, tail, nargout=0
        )

        for item in os.scandir(self.QARDirIn):
            if not item.name.startswith("ICDs"):
                # Prints only text file present in My Folder
                os.remove(item)

    def download_blob_to_file(self, file):
        file = file.replace(
            "/blobServices/default/containers/"
            + self.AIRLINE_FLIGHT_DATA_CONTAINER
            + "/blobs/",
            "",
        )
        # print("Downloading: ", file, flush=True)
        blob_client = self.blob_client.get_blob_client(
            container=self.AIRLINE_FLIGHT_DATA_CONTAINER, blob=file
        )

        tokens = file.split("/")
        airline = tokens[1]
        tail = tokens[2]
        file_name = tokens.pop()

        with open(file=(self.QARDirIn + "/" + file_name), mode="wb") as sample_blob:
            download_stream = blob_client.download_blob()
            sample_blob.write(download_stream.readall())
        print("File downloaded successfully", flush=True)

        self.unzip(self.QARDirIn)
        self.decode(airline, tail)
        self.upload_blob_file(self.blob_client, self.FLIGHT_RECORDS_CONTAINER)

    def upload_blob_file(self, client, container_name):
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
                    raw_type = "qar"
                    path = (
                        raw_type
                        + "/"
                        + airline
                        + "/"
                        + date
                        + "/"
                        + tail
                        + "/"
                        + item.name
                    )
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

    def read_from_service_bus(self):
        print("Listening...", flush=True)
        try:
            # Read from service bus runtime-override topic to check if runtime must be overriden for app restart
            self.read_sb_topic()

            with ServiceBusAdministrationClient.from_connection_string(
                self.QUEUE_NAMESPACE_CONN
            ) as client:
                count = client.get_queue_runtime_properties(
                    self.QAR_DECODE_QUEUE
                ).active_message_count

            print("Message count: ", count, flush=True)
            while count > 0:
                # self.restart_program()

                # Install ICDs
                self.download_icds()
                # Install runtime package
                self.download_package()
                self.QAR_Decode = importlib.import_module("QAR_Decode")
                # self.QAR_Decode.initialize_runtime(["-nojvm"])

                if self.my_QAR_Decode == None:
                    self.my_QAR_Decode = self.QAR_Decode.initialize()

                # Read from service bus qar-decode queue
                count = self.read_sb_queue()

        except Exception as e:
            print("Error reading from service bus: ", e, flush=True)
            # self.my_QAR_Decode.terminate()
            self.rollback()

    def read_sb_topic(self):
        try:
            with ServiceBusClient.from_connection_string(
                self.TOPIC_NAMESPACE_CONN
            ) as client:
                # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
                # Default is None; to receive forever.
                with client.get_subscription_receiver(
                    topic_name=self.QAR_DECODE_TOPIC,
                    subscription_name=self.TOPIC_SUBSCRIPTION,
                    max_wait_time=5,
                ) as receiver:
                    for msg in receiver:  # ServiceBusReceiver instance is a generator.
                        print(str(msg))
                        # Read each message
                        data = json.loads(str(msg))
                        pkg = data["package"]
                        print("Message read from topic: ", pkg, flush=True)
                        print("Overriding package...", flush=True)
                        runtime = self.get_runtime()
                        print("Current package...", runtime["package"], flush=True)
                        runtime["package"] = pkg
                        print("New package...", runtime["package"], flush=True)
        except Exception as e:
            print("Error reading from sb topic: ", e, flush=True)
            # self.my_QAR_Decode.terminate()
            self.rollback()

            # receiver.complete_message(msg)

    def read_sb_queue(self):
        try:
            with ServiceBusClient.from_connection_string(
                self.QUEUE_NAMESPACE_CONN
            ) as client:
                # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
                # Default is None; to receive forever.
                with client.get_queue_receiver(
                    self.QAR_DECODE_QUEUE, max_wait_time=30
                ) as receiver:
                    for msg in receiver:  # ServiceBusReceiver instance is a generator.
                        # Read each message
                        data = json.loads(str(msg))
                        subject = data["subject"]
                        msg_id = data["id"]
                        print("Message id: ", msg_id, flush=True)
                        print("Message subject: ", subject, flush=True)

                        # Download qar file from path in message
                        #self.download_blob_to_file(subject)
                        #receiver.complete_message(msg)
                        # If it is desired to halt receiving early, one can break out of the loop here safely.
                        with ServiceBusAdministrationClient.from_connection_string(
                            self.QUEUE_NAMESPACE_CONN
                        ) as client:
                            count = client.get_queue_runtime_properties(
                                self.QAR_DECODE_QUEUE
                            ).active_message_count
                            print("Messages left in queue: ", count, flush=True)
            return count
        except Exception as e:
            print("Error reading from sb queue: ", e, flush=True)
            #self.my_QAR_Decode.terminate()
            self.rollback()

    def unzip(self, qar_dir_in):
        extension = ".zip"
        # os.chdir(qar_dir_in)

        for item in os.scandir(qar_dir_in):  # loop through items in dir
            if item.name.endswith(extension):  # check for ".zip" extension
                # file_name = os.path.abspath(item)  # get full path of files
                zip_ref = zipfile.ZipFile(item)  # create zipfile object
                # print(zip_ref.filename)
                zip_ref.extractall(qar_dir_in)  # extract file to dir
                zip_ref.close()  # close file
                os.remove(item)  # delete zipped file
                raw = qar_dir_in + "/raw_" + str(uuid.uuid4()) + ".dat"
                os.rename(qar_dir_in + "/raw.dat", raw)

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

            print("Package installed")
        except Exception as e:
            print("Error installing package: ", e, flush=True)

    def restart_program(self):
        """Restarts the current program.
        Note: this function does not return. Any cleanup action (like
        saving data) must be done before calling this function."""
        print("Terminating... ", flush=True)
        python = sys.executable
        os.execl(python, python, *sys.argv)


if __name__ == "__main__":
    decoder = Decoder()
    schedule.every(1).seconds.do(decoder.read_from_service_bus)

    while True:
        schedule.run_pending()
        time.sleep(1)
