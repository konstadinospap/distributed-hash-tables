import hashlib
import pickle
import socket
import threading
import time

import pandas as pd


class ChordNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.node_id = self.hash_key(f"{host}:{port}")
        self.data = {}
        self.successor = (self.host, self.port)
        self.predecessor = None
        self.lock = threading.Lock()

    def hash_key(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** 160)

    def start(self):
        threading.Thread(target=self.listen).start()

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_connection, args=(conn,)).start()

    def handle_connection(self, conn):
        with conn:
            data = conn.recv(4096)
            if data:
                message = pickle.loads(data)
                command = message.get("command")
                if command == "find_successor":
                    id = message.get("id")
                    successor = self.find_successor(id)
                    conn.sendall(pickle.dumps(successor))
                elif command == "insert_key":
                    key = message.get("key")
                    value = message.get("value")
                    self.insert_key(key, value)
                elif command == "lookup":
                    key = message.get("key")
                    value = self.lookup(key)
                    conn.sendall(pickle.dumps(value))
                elif command == "delete_key":
                    key = message.get("key")
                    self.delete_key(key)
                elif command == "update_key":
                    key = message.get("key")
                    value = message.get("value")
                    self.update_key(key, value)
                elif command == "notify":
                    predecessor = message.get("predecessor")
                    self.notify(predecessor)
                # Προσθήκη επιπλέον εντολών αν χρειάζεται

    def find_successor(self, id):
        with self.lock:
            if self.predecessor and self.predecessor[1] < id <= self.node_id:
                return (self.host, self.port)
            else:
                return self.successor

    def join(self, existing_node):
        if existing_node:
            self.predecessor = None
            self.successor = self.remote_find_successor(existing_node, self.node_id)
        else:
            self.predecessor = (self.host, self.port)
            self.successor = (self.host, self.port)

    def remote_find_successor(self, node, id):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "find_successor", "id": id}
            s.sendall(pickle.dumps(message))
            data = s.recv(4096)
            return pickle.loads(data)

    def insert_key(self, key, value):
        id = self.hash_key(key)
        successor = self.find_successor(id)
        if successor == (self.host, self.port):
            with self.lock:
                self.data[id] = value
        else:
            self.remote_insert_key(successor, key, value)

    def remote_insert_key(self, node, key, value):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "insert_key", "key": key, "value": value}
            s.sendall(pickle.dumps(message))

    def lookup(self, key):
        id = self.hash_key(key)
        successor = self.find_successor(id)
        if successor == (self.host, self.port):
            with self.lock:
                return self.data.get(id, None)
        else:
            return self.remote_lookup(successor, key)

    def remote_lookup(self, node, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "lookup", "key": key}
            s.sendall(pickle.dumps(message))
            data = s.recv(4096)
            return pickle.loads(data)

    def delete_key(self, key):
        id = self.hash_key(key)
        successor = self.find_successor(id)
        if successor == (self.host, self.port):
            with self.lock:
                if id in self.data:
                    del self.data[id]
        else:
            self.remote_delete_key(successor, key)

    def remote_delete_key(self, node, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "delete_key", "key": key}
            s.sendall(pickle.dumps(message))

    def update_key(self, key, value):
        id = self.hash_key(key)
        successor = self.find_successor(id)
        if successor == (self.host, self.port):
            with self.lock:
                self.data[id] = value
        else:
            self.remote_update_key(successor, key, value)

    def remote_update_key(self, node, key, value):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "update_key", "key": key, "value": value}
            s.sendall(pickle.dumps(message))

    def notify(self, predecessor):
        with self.lock:
            if (self.predecessor is None or
                self.predecessor[0] < predecessor[0] < self.node_id):
                self.predecessor = predecessor

    def stabilize(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.successor)
            message = {"command": "get_predecessor"}
            s.sendall(pickle.dumps(message))
            data = s.recv(4096)
            x = pickle.loads(data)
            if x and self.node_id < x[0] < self.successor[0]:
                self.successor = x
            self.remote_notify(self.successor, (self.node_id, (self.host, self.port)))

    def remote_notify(self, node, predecessor):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node)
            message = {"command": "notify", "predecessor": predecessor}
            s.sendall(pickle.dumps(message))

    def node_leave(self):
        with self.lock:
            if self.predecessor:
                self.remote_update_successor(self.predecessor, self.successor)
            if self.successor:
                self.remote_update_predecessor(self.successor, self.predecessor)
            # Μεταφορά των δεδομένων στον διάδοχο




def insert_data_from_csv(file_path):
    # Ανάγνωση του CSV αρχείου σε ένα DataFrame
    df = pd.read_csv(file_path)
    
    # Επανάληψη μέσω των γραμμών του DataFrame
    for index, row in df.iterrows():
        # Χρησιμοποιούμε το 'loc_country' ως κλειδί
        key = row['loc_country']
        # Μπορούμε να αποθηκεύσουμε ολόκληρη τη γραμμή ως τιμή ή συγκεκριμένα πεδία
        value = row.to_dict()  # Αποθηκεύουμε ολόκληρη τη γραμμή ως λεξικό
        # Εισάγουμε το κλειδί και την τιμή στο δίκτυο Chord μέσω του πρώτου κόμβου
        nodes[0].insert_key(key, value)
        #print(f'Inserted {key}: {value}')

# το κύριο πρόγραμμα


# Φόρτωση του συνόλου δεδομένων
# Δημιουργία 10 κόμβων Chord
nodes = []
base_port = 5300
for i in range(6):
    port = base_port + i
    node = ChordNode('localhost', port)
    if i == 0:
        node.join(None)  # Ο πρώτος κόμβος δημιουργεί το δίκτυο
    else:
        node.join(('localhost', base_port))  # Οι υπόλοιποι κόμβοι εντάσσονται στο δίκτυο
    node.start()
    nodes.append(node)
    time.sleep(1)  # Αναμονή για την εκκίνηση του κόμβου

start_time = time.time()
insert_data_from_csv('coffee_analysis.csv')
end_time = time.time()

print(f"time inserts: {end_time - start_time:.4f} ")




start_time = time.time()
value = node.lookup('USA')
end_time = time.time()

print(f"time search: {end_time - start_time:.4f} ")



start_time = time.time()
value = node.delete_key("USA")
end_time = time.time()

print(f"time remove: {end_time - start_time:.4f} ")