import logging
import random
import threading
import time

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