import hashlib
import pickle
import socket
import threading
import time
import pandas as pd

class PastryNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.node_id = self.hash_key(f"{host}:{port}")[:8]  # Μικρότερο hash για Pastry (prefix-based)
        self.data = {}
        self.leaf_set = []  # Σύνολο των πιο κοντινών κόμβων
        self.routing_table = {}  # Πίνακας δρομολόγησης
        self.lock = threading.Lock()

    def hash_key(self, key):
        return hashlib.sha1(key.encode()).hexdigest()

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
                if command == "insert_key":
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
                elif command == "join":
                    new_node_address = message.get("node_address")
                    self.update_routing(new_node_address)
                elif command == "leave":
                    node_address = message.get("node_address")
                    self.remove_node(node_address)

    def insert_key(self, key, value):
        id = self.hash_key(key)[:8]  # Παίρνουμε τα πρώτα 8 bytes για σύγκριση προθεμάτων
        responsible_node = self.route(id)
        if responsible_node == (self.host, self.port):
            with self.lock:
                self.data[id] = value
        else:
            self.remote_insert_key(responsible_node, key, value)

    def remote_insert_key(self, node_address, key, value):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node_address)
            message = {"command": "insert_key", "key": key, "value": value}
            s.sendall(pickle.dumps(message))

    def lookup(self, key):
        id = self.hash_key(key)[:8]
        responsible_node = self.route(id)
        if responsible_node == (self.host, self.port):
            with self.lock:
                return self.data.get(id, None)
        else:
            return self.remote_lookup(responsible_node, key)

    def remote_lookup(self, node_address, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node_address)
            message = {"command": "lookup", "key": key}
            s.sendall(pickle.dumps(message))
            data = s.recv(4096)
            return pickle.loads(data)

    def delete_key(self, key):
        id = self.hash_key(key)[:8]
        responsible_node = self.route(id)
        if responsible_node == (self.host, self.port):
            with self.lock:
                if id in self.data:
                    del self.data[id]
        else:
            self.remote_delete_key(responsible_node, key)

    def remote_delete_key(self, node_address, key):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(node_address)
            message = {"command": "delete_key", "key": key}
            s.sendall(pickle.dumps(message))

    def route(self, key_id):
        """ Επιστρέφει τον κόμβο που είναι υπεύθυνος για το κλειδί """
        closest_node = (self.host, self.port)
        closest_distance = self.distance(self.node_id, key_id)

        with self.lock:
            for node_address in self.leaf_set + list(self.routing_table.values()):
                node_id = self.hash_key(f"{node_address[0]}:{node_address[1]}")[:8]
                dist = self.distance(node_id, key_id)
                if dist < closest_distance:
                    closest_node = node_address
                    closest_distance = dist

        return closest_node

    def update_routing(self, new_node_address):
        """ Προσθήκη του νέου κόμβου στον πίνακα δρομολόγησης """
        new_node_id = self.hash_key(f"{new_node_address[0]}:{new_node_address[1]}")[:8]
        with self.lock:
            if new_node_address not in self.leaf_set:
                self.leaf_set.append(new_node_address)
            prefix_length = len(self.common_prefix(self.node_id, new_node_id))
            self.routing_table[prefix_length] = new_node_address

    def remove_node(self, node_address):
        """ Αφαιρεί έναν κόμβο από το δίκτυο """
        with self.lock:
            if node_address in self.leaf_set:
                self.leaf_set.remove(node_address)
            for key, value in list(self.routing_table.items()):
                if value == node_address:
                    del self.routing_table[key]

    def common_prefix(self, id1, id2):
        """ Υπολογίζει το κοινό πρόθεμα μεταξύ δύο αναγνωριστικών """
        prefix = ""
        for c1, c2 in zip(id1, id2):
            if c1 == c2:
                prefix += c1
            else:
                break
        return prefix

    def distance(self, id1, id2):
        """ Υπολογίζει την απόσταση μεταξύ δύο αναγνωριστικών """
        return abs(int(id1, 16) - int(id2, 16))

def insert_data_from_csv(file_path):
    """ Εισαγωγή δεδομένων από αρχείο CSV στο δίκτυο Pastry """
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        key = row['loc_country']
        value = row.to_dict()
        nodes[0].insert_key(key, value)


# Κύριο πρόγραμμα για προσομοίωση του δικτύου Pastry
nodes = []
base_port = 5300

for i in range(6):
    port = base_port + i
    node = PastryNode('localhost', port)
    if i > 0:
        node.update_routing(nodes[0].node_id)  # Σύνδεση με το πρώτο node
    node.start()
    nodes.append(node)
    time.sleep(1)


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