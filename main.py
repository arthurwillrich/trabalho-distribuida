import socket

class ReliableComm:
    def __init__(self, process_id):
        self.process_id = process_id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('localhost', process_id))
        self.socket.listen()

    def send(self, dest_id, message):
        dest_address = ('localhost', dest_id)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(dest_address)
            sock.send(message.encode())

    def receive(self):
        connection, address = self.socket.accept()
        message = connection.recv(1024).decode()
        return message
