import os

import pulsar

client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer("bag-events-unzip")

path = os.path.join(os.getcwd(), "bag")
print(f"Path: {path}")
producer.send(path.encode("utf8"))
