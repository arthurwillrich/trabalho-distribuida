import random
import time
import socket
from threading import Thread, Lock
from queue import Queue

class SistemaEntrega:
    def __init__(self, num_replicas):
        self.timestamp = [0] * num_replicas
        self.replicas = []  # Lista de réplicas (sockets)
        self.buffer_unicast = []
        self.buffer_broadcast = []
        self.queue_unicast = Queue()  # Fila para sincronização do acesso ao buffer_unicast
        self.queue_broadcast = Queue()  # Fila para sincronização do acesso ao buffer_broadcast
        self.mutex_unicast = Lock()  # Mutex para exclusão mútua no acesso ao buffer_unicast

    def conectar_replicas(self):
        for i in range(len(self.timestamp)):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('localhost', 6000 + i))
            sock.listen(1)
            self.replicas.append(sock)

    def repassar_mensagem(self, remetente, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.queue_broadcast.put((remetente, mensagem))  # Insere a mensagem na fila
        self.entregar_mensagens_broadcast()  # Chamada para entregar as mensagens broadcast

    def enviar_mensagem_unicast(self, remetente, destinatario, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.queue_unicast.put((remetente, destinatario, mensagem))  # Insere a mensagem na fila
        self.entregar_mensagens_unicast(destinatario)  # Chamada para entregar as mensagens unicast

    def enviar_mensagem_broadcast(self, remetente, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.queue_broadcast.put((remetente, mensagem))  # Insere a mensagem na fila
        self.entregar_mensagens_broadcast()  # Chamada para entregar as mensagens broadcast

    def entregar_mensagens_unicast(self, destinatario):
        self.mutex_unicast.acquire()  # Adquire o bloqueio mutex_unicast
        while not self.queue_unicast.empty():
            remetente, dest, mensagem = self.queue_unicast.get()
            self.buffer_unicast.append((self.timestamp[remetente], remetente, dest, mensagem))
        self.buffer_unicast.sort(key=lambda x: x[0])  # Ordenar pelo timestamp
        for timestamp, remetente, dest, mensagem in self.buffer_unicast:
            if dest == destinatario:
                self.delayed_print(f"Entrega unicast para {destinatario}: {remetente} -> {mensagem}")
                self.timestamp[destinatario] += 1  # Atualiza o relógio lógico do destinatário
        self.buffer_unicast = [msg for msg in self.buffer_unicast if msg[2] != destinatario]  # Remove as mensagens entregues
        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")
        self.mutex_unicast.release()  # Libera o bloqueio mutex_unicast

    def entregar_mensagens_broadcast(self):
        while not self.queue_broadcast.empty():
            remetente, mensagem = self.queue_broadcast.get()
            self.buffer_broadcast.append((self.timestamp[remetente], remetente, mensagem))
        self.buffer_broadcast.sort(key=lambda x: x[0])  # Ordenar pelo timestamp
        while len(self.buffer_broadcast) > 0:
            timestamp, remetente, mensagem = self.buffer_broadcast.pop(0)
            self.delayed_print(f"Entrega broadcast: {remetente} -> {mensagem}")
            for replica in self.replicas:
                if replica != self.replicas[remetente]:
                    self.repassar_mensagem(remetente, mensagem)  # Repassa a mensagem para as demais réplicas
        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")

    def delayed_print(self, message):
        time.sleep(random.uniform(0, 1))
        print(message)

    def encerrar_replicas(self):
        # Aguardar até que os buffers estejam vazios
        while not self.queue_unicast.empty() or not self.queue_broadcast.empty():
            time.sleep(1)  # Aguardar por um segundo antes de verificar novamente

        # Fechar os sockets das réplicas
        for replica in self.replicas:
            replica.close()

    # Função para executar uma bateria de testes de mensagens unicast
def bateria_teste_unicast(sistema, remetente, destinatario):
    for i in range(3):
        mensagem = f"Mensagem {i + 1} de {remetente} para {destinatario}"
        sistema.enviar_mensagem_unicast(remetente, destinatario, mensagem)
        time.sleep(random.uniform(0.1, 0.5))

# Função para executar uma bateria de testes de mensagens broadcast
def bateria_teste_broadcast(sistema, remetente):
    for i in range(3):
        mensagem = f"Mensagem {i + 1} de {remetente} (broadcast)"
        sistema.enviar_mensagem_broadcast(remetente, mensagem)
        time.sleep(random.uniform(0.1, 0.5))

# Restante do código

# Bateria de Testes 1
print("Bateria de Testes 1")
sistema1 = SistemaEntrega(4)
sistema1.conectar_replicas()

t1 = Thread(target=bateria_teste_unicast, args=(sistema1, 0, 1))
t4 = Thread(target=bateria_teste_unicast, args=(sistema1, 1, 3))

t1.start()
t4.start()

t1.join()
t4.join()

sistema1.encerrar_replicas()
