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
import signal
from multiprocessing import Pool
import multiprocessing
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


# -----------------------------------------------

load_dotenv()
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
AIRLINE_FLIGHT_DATA_PENDING_CONTAINER = os.getenv(
    "AIRLINE_FLIGHT_DATA_PENDING_CONTAINER"
)
ANALYTICS_PENDING_CONTAINER = os.getenv("ANALYTICS_PENDING_CONTAINER")
icds = None
package = None

AIRLINE_FLIGHT_DATA_CONTAINER = os.getenv("AIRLINE_FLIGHT_DATA_CONTAINER")
AIRLINE_FLIGHT_DATA_QUEUE = os.getenv("AIRLINE_FLIGHT_DATA_PENDING_QUEUE")
blob_client = None
queue_client = None
absolute_path = Path.cwd()

relative_path = "input"
QARDirIn = absolute_path / relative_path
qar_dir_in = str(Path(QARDirIn))

relative_path = "output"
OutDirIn = absolute_path / relative_path
out_dir_in = str(Path(OutDirIn))

relative_path = "scripts"
ScriptsDirIn = absolute_path / relative_path
scripts_dir_in = str(Path(ScriptsDirIn))

relative_path = ""
RootDir = absolute_path / relative_path
root_dir = str(Path(RootDir))

logging.basicConfig()
schedule_logger = logging.getLogger("schedule")
schedule_logger.setLevel(level=logging.DEBUG)


def auth_blob_client():
    global blob_client
    blob_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)


def auth_queue_client():
    # Setup Base64 encoding and decoding functions
    global queue_client
    queue_client = QueueClient.from_connection_string(
        STORAGE_CONNECTION_STRING,
        AIRLINE_FLIGHT_DATA_QUEUE,
        message_decode_policy=TextBase64DecodePolicy(),
    )


def get_runtime():
    try:
        client = blob_client.get_blob_client(
            container=ANALYTICS_PENDING_CONTAINER, blob="config/runtime.json"
        )

        downloader = client.download_blob(max_concurrency=1, encoding="UTF-8")
        blob_text = downloader.readall()
        runtime = json.loads(blob_text)
        global icds
        icds = runtime["icds"]
        global package
        package = runtime["package"]
        print("Runtime: ", runtime)
    except Exception as e:
        print("Error retrieving runtime config: ", e, flush=True)


def read_from_queue():
    print("Listening...", flush=True)
    try:
        global queue_client
        # properties = queue_client.get_queue_properties()
        # count = properties.approximate_message_count
        count = queue_client.peek_messages(max_messages=5)
        print("Message count: ", len(count), flush=True)
        if len(list(count)) > 0:
            messages = queue_client.receive_messages(
                max_messages=32, visibility_timeout=1800
            )

            # Install runtime package
            download_package()
            QAR_Decode = importlib.import_module("QAR_Decode")
            QAR_Decode.initialize_runtime(["-nojvm"])
            my_QAR_Decode = QAR_Decode.initialize()

            # Install ICDs
            download_icds()

            return list(messages)

        return []
        """
        # Terminate package
        my_QAR_Decode.terminate()

        # Get queue message count again
        count = queue_client.peek_messages(max_messages=5)
        """
    except Exception as e:
        print("Error processing batch: ", e, flush=True)
        my_QAR_Decode.terminate()


def download_icds():
    try:
        blob_client = BlobServiceClient.from_connection_string(
            STORAGE_CONNECTION_STRING
        )
        client = blob_client.get_blob_client(
            container=ANALYTICS_PENDING_CONTAINER, blob=icds
        )
        with open(file=(root_dir + "/ICDs.zip"), mode="wb") as sample_blob:
            download_stream = client.download_blob()
            sample_blob.write(download_stream.readall())
        print("ICDs retrieved", flush=True)
        zip_ref = zipfile.ZipFile(root_dir + "/ICDs.zip")  # create zipfile object
        # print(zip_ref.filename)
        zip_ref.extractall(root_dir)  # extract file to dir
        zip_ref.close()  # close file
        os.remove(root_dir + "/ICDs.zip")  # delete zipped file
    except Exception as e:
        print("Error retrieving ICDs: ", e, flush=True)


def download_package():
    try:
        blob_client = BlobServiceClient.from_connection_string(
            STORAGE_CONNECTION_STRING
        )
        client = blob_client.get_blob_client(
            container=ANALYTICS_PENDING_CONTAINER, blob=package
        )

        with open(file=(scripts_dir_in + "/QAR_Decode.zip"), mode="wb") as sample_blob:
            download_stream = client.download_blob()
            sample_blob.write(download_stream.readall())
        print("Package retrieved", flush=True)
        zip_ref = zipfile.ZipFile(
            scripts_dir_in + "/QAR_Decode.zip"
        )  # create zipfile object
        zip_ref.extractall(scripts_dir_in)  # extract file to dir
        time.sleep(2)
        zip_ref.close()  # close file
        os.remove(scripts_dir_in + "/QAR_Decode.zip")  # delete zipped file
        install_package(scripts_dir_in)
    except Exception as e:
        print("Error retrieving package: ", e, flush=True)

def install_package(scripts_dir_in):
        try:
            os.chdir(scripts_dir_in + "/for_redistribution_files_only")
            process = subprocess.Popen(["python", "setup.py", "install"])
            process.wait()
            print("Package installed")
            os.chdir(root_dir)
        except Exception as e:
            print("Error installing package: ", e, flush=True)


def unzip(qar_dir_in):
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
                # os.rename(qar_dir_in + "/raw.dat", raw)
    except Exception as e:
        print("Error deleting unzipped file: ", e, flush=True)


def decode(airline, tail, qar_dir_in, out_dir_in):
    QAR_Decode = importlib.import_module("QAR_Decode")
    QAR_Decode.initialize_runtime(["-nojvm"])
    my_QAR_Decode = QAR_Decode.initialize()
    # Decode binary
    my_QAR_Decode.QAR_Decode(qar_dir_in, out_dir_in, airline, tail, nargout=0)

    for item in os.scandir(qar_dir_in):
        if not item.name.startswith("ICDs"):
            # Prints only text file present in My Folder
            os.remove(item)
    my_QAR_Decode.terminate()


def process_queue(msg):
    try:
        print(msg.content)

        # Set temp dir paths
        _qar_dir_in = qar_dir_in + "_" + str(uuid.uuid4())
        _out_dir_in = out_dir_in + "_" + str(uuid.uuid4())

        # Create temp dirs
        os.makedirs(_qar_dir_in)
        os.makedirs(_out_dir_in)
        os.chmod(_qar_dir_in, 0o777)
        os.chmod(_out_dir_in, 0o777)
        os.chmod(root_dir + "/ICDs", 0o777)

        shutil.copytree(root_dir + "/ICDs", _qar_dir_in + "/ICDs")
        os.chmod(_qar_dir_in + "/ICDs", 0o777)

        # Read each message
        data = json.loads(msg.content)
        file = data["file_path"]
        print("", flush=True)
        print("Message read from queue: ", file, flush=True)

        # Download qar file from path in message
        download_blob_to_file(file, _qar_dir_in, _out_dir_in)

        queue_client = QueueClient.from_connection_string(
            STORAGE_CONNECTION_STRING,
            AIRLINE_FLIGHT_DATA_QUEUE,
            message_decode_policy=TextBase64DecodePolicy(),
        )

        queue_client.delete_message(msg)

        # Delete temp dirs
        shutil.rmtree(_qar_dir_in)
        shutil.rmtree(_out_dir_in)
    except Exception as e:
        print("Exception: ", e, flush=True)
        rollback(qar_dir_in, out_dir_in)


def download_blob_to_file(file, qar_dir_in, out_dir_in):
    # print("Downloading: ", file, flush=True)
    blob_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    client = blob_client.get_blob_client(
        container=AIRLINE_FLIGHT_DATA_PENDING_CONTAINER, blob=file
    )
    tokens = file.split("/")
    airline = tokens[1]
    tail = tokens[2]
    file_name = tokens.pop()

    with open(file=(qar_dir_in + "/" + file_name), mode="wb") as sample_blob:
        download_stream = client.download_blob()
        sample_blob.write(download_stream.readall())
    print("File downloaded successfully", flush=True)

    unzip(qar_dir_in)
    decode(airline, tail, qar_dir_in, out_dir_in)
    upload_blob_file(blob_client, AIRLINE_FLIGHT_DATA_CONTAINER, out_dir_in)


def upload_blob_file(client, container_name, out_dir_in):
    container_client = client.get_container_client(container=container_name)
    extension = ".csv"
    for item in os.scandir(out_dir_in):  # loop through items in
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
                    raw_type + "/" + airline + "/" + tail + "/" + date + "/" + item.name
                )
                with open(file=(item), mode="rb") as data:
                    container_client.upload_blob(name=path, data=data, overwrite=True)
                    print("File uploaded successfully", flush=True)


def rollback(qar_dir_in, out_dir_in):
    try:
        shutil.rmtree(qar_dir_in)
        shutil.rmtree(out_dir_in)
    except Exception as e:
        print("Error cleaning: ", e, flush=True)


def start():
    auth_blob_client()
    auth_queue_client()
    get_runtime()

    messages = read_from_queue()
    if len(messages):
        print("Number of CPUs: ", multiprocessing.cpu_count(), flush=True)
        pool = Pool()
        pool.map(process_queue, messages)
        pool.close()
        pool.join()
        print("pool finished", flush=True)
        # decoder.terminate_package()


if __name__ == "__main__":
    schedule.every(1).minutes.do(start)

    while True:
        schedule.run_pending()
        time.sleep(1)
