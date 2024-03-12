import logging
import random
import threading
import time

#Eventual Consistency

class EventualConsistencyKVStore(KeyValueStore):
    def __init__(self, replica):
        super().__init__()
        self.replica = replica
        self.pending_updates = {address: []
                                for address in self.replica.replica_addresses}

        threading.Thread(target=self.gossip_thread).start()

    def set(self, key, value):
        super().set(key, value)

        for address in self.replica.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                self.pending_updates.setdefault(
                    address, []).append((key, value))

        return "Key-value pair added"

    def gossip_thread(self):
        while True:
            time.sleep(self.replica.gossip_interval)
            self.send_updates()

    def send_updates(self):
        for target_replica, updates in self.pending_updates.items():
            if updates:
                data = "update " + \
                    " ".join([f"{key} {value}" for key, value in updates])
                send(target_replica, data)

                self.pending_updates[target_replica] = []
                
#Linear Consistency      
class LinearConsistencyKVStore(KeyValueStore):
    def __init__(self, replica):
        super().__init__()
        self.replica = replica

    def set(self, key, value):
        super().set(key, value)

        # Keep track of which replicas have acknowledged the update
        acknowledgements = []

        # Send new key-value pair to each replica
        for address in self.replica.replica_addresses:
            if address != (self.replica.host, self.replica.port):
                data = f"update {key} {value}"
                acknowledgements.append(self.send_updates(address, data))

        # Wait for all replicas to acknowledge the update
        for acknowledgement in acknowledgements:
            acknowledgement.wait()

        return "Key-value pair added"

    # Send updates to the target replica
    def send_updates(self, target_replica, data):
        acknowledge_event = threading.Event()

        # Set the acknowledge event when the acknowledgement is received
        def callback():
            simulate_latency()
            acknowledge_event.set()

        send(target_replica, data, callback=callback)

        return acknowledge_event
