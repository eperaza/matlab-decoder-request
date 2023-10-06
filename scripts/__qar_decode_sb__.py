import asyncio
from azure.servicebus import ServiceBusClient
import os

NAMESPACE_CONNECTION_STR = "Endpoint=sb://sbns-tspservices-test.servicebus.windows.net/;SharedAccessKeyName=tsp-services;SharedAccessKey=hm3JelNwhfXPRk6e/iWnBi4ERE6KDTuDC+ASbFskXsA=;EntityPath=qar-decode"
QUEUE_NAME = "qar-decode"

def run():
    with ServiceBusClient.from_connection_string(NAMESPACE_CONNECTION_STR) as client:
    # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
    # Default is None; to receive forever.
        with client.get_queue_receiver(QUEUE_NAME, max_wait_time=30) as receiver:
            for msg in receiver:  # ServiceBusReceiver instance is a generator.
                print(str(msg))
                receiver.complete_message(msg)
                # If it is desired to halt receiving early, one can break out of the loop here safely.

if __name__ == "__main__":
    run()