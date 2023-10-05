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
import json_stream
import signal

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import (
    QueueServiceClient,
    QueueClient,
    QueueMessage,
    TextBase64DecodePolicy,
)

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
        self.AIRLINE_FLIGHT_DATA_CONTAINER = os.getenv(
            "AIRLINE_FLIGHT_DATA_CONTAINER"
        )
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
        try:
            # Decode binary
            self.my_QAR_Decode.QAR_Decode(
                self.QARDirIn, self.OutDirIn, airline, tail, nargout=0
            )

            for item in os.scandir(self.QARDirIn):
                if not item.name.startswith("ICDs"):
                    # Prints only text file present in My Folder
                    os.remove(item)
        except Exception as e:
            print("Decoding failed: ", e, flush=True)
            self.rollback()

    def download_blob_to_file(self, file):
        try:
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

        except Exception as e:
            print("Exception: ", e, flush=True)
            self.rollback()

    def upload_blob_file(self, client, container_name):
        try:
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
        except Exception as e:
            print("Error uploading file: ", e, flush=True)
            self.rollback()

    def read_from_queue(self):
        print("Listening...", flush=True)
        try:
            properties = self.queue_client.get_queue_properties()
            # count = properties.approximate_message_count
            count = self.queue_client.peek_messages(max_messages=5)
            print("Message count: ", len(count), flush=True)
            while len(count) > 0:
                print(len(count))
                messages = self.queue_client.receive_messages(
                    max_messages=32, visibility_timeout=1800
                )
                # Install ICDs
                self.download_icds()
                # Install runtime package
                self.download_package()
                self.QAR_Decode = importlib.import_module("QAR_Decode")
                #self.QAR_Decode.initialize_runtime(["-nojvm"])
                self.my_QAR_Decode = self.QAR_Decode.initialize()
                # Read message queue
                for msg in messages:
                    # Read each message
                    data = json.loads(msg.content)
                    file = data["file_path"]
                    print("", flush=True)
                    print("Message read from queue: ", file, flush=True)
                    # Download qar file from path in message
                    self.download_blob_to_file(file)
                    self.queue_client.delete_message(msg)
                    # print("Messages processed in this run: ", len(count), flush=True)
                # Delete package
                self.my_QAR_Decode.terminate()
                
                #shutil.rmtree(self.ScriptsDirIn + "/for_redistribution_files_only")
                # Get queue message count again
                count = self.queue_client.peek_messages(max_messages=5)
        except Exception as e:
            print("Error processing batch: ", e, flush=True)
            self.my_QAR_Decode.terminate()
            self.rollback()

        # terminate package
        # self.my_QAR_Decode.terminate()

        # Upload blob to storage

    def unzip(self, qar_dir_in):
        try:
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
        except Exception as e:
            print("Error deleting unzipped file: ", e, flush=True)
            self.rollback()

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

if __name__ == "__main__":
    decoder = Decoder()
    schedule.every(1).minutes.do(decoder.read_from_queue)

    while True:
        schedule.run_pending()
        time.sleep(1)
