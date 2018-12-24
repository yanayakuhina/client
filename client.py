import socket
import time


class ClientError(Exception):
    pass


class Client:
    def __init__(self, ip, port, timeout=None):


        self.socket = socket.create_connection((ip, port), timeout)



    def put(self, metric, val, timestamp=None):
        if not timestamp:
            timestamp = str(int(time.time()))
        data = f"put {metric} {val} {timestamp}\n"

        data_bytes = data.encode("utf8")
        self.sendall(data_bytes)


        data = self.recv()
        if data == b'ok\n\n':
            pass
        elif data == b'error\nwrong command\n\n':
            raise ClientError

    def get(self, metric):
        msg = f"get {metric}\n"
        msg_bytes = msg.encode("utf8")
        self.sendall(msg_bytes)


        answ = bytearray()
        while True:
            data = self.socket.recv(1024)
            if not data:
                raise ClientError


            answ.extend(data)
            if answ[-2:] == b'\n\n':
                break


        if not data.startswith(b'ok\n'):
            raise ClientError


        data = data[len(b'ok\n'):-2]
        answ = data.decode("utf8")


        dict_metrics = {}
        for line in answ.splitlines():
            metric, metic_list, timestamp = line.split()

            if metric not in dict_metrics:
                dict_metrics[metric] = []
            dict_metrics[metric].append((int(timestamp), float(metic_list)))

        for metric, metic_list in dict_metrics.items():
            dict_metrics[metric] = sorted(metic_list, key=lambda timestamp_val: timestamp_val[0])

        return dict_metrics

    def recv(self):
        try:
            return self.socket.recv(1024)
        except socket.timeout as e:
            raise ClientError

    def sendall(self, data):
        try:
            self.socket.sendall(data)
        except socket.timeout as e:
            raise ClientError
