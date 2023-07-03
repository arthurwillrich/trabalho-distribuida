import random
import time
import socket
from threading import Thread, Lock
from queue import Queue

class SistemaEntrega:
    def __init__(self, num_replicas):
        self.num_replicas = num_replicas
        self.timestamp = [0] * num_replicas
        self.replicas = []  # Lista de réplicas (sockets)
        self.buffer_unicast = []
        self.buffer_broadcast = []
        self.queue_unicast = Queue()  # Fila para sincronização do acesso ao buffer_unicast
        self.queue_broadcast = Queue()  # Fila para sincronização do acesso ao buffer_broadcast
        self.mutex_unicast = Lock()  # Mutex para exclusão mútua no acesso ao buffer_unicast
        self.mutex_broadcast = Lock()  # Mutex para exclusão mútua no acesso ao buffer_broadcast
        self.mutex_broadcast_ordem_total = Lock()  # Novo mutex para exclusão mútua no acesso ao buffer_broadcast ordenado

    def entregar_mensagens_broadcast_ordem_total(self):
        self.mutex_broadcast_ordem_total.acquire()  # Adquire o bloqueio mutex_broadcast_ordem_total

        while not self.queue_broadcast.empty():
            remetente, mensagem = self.queue_broadcast.get()
            self.buffer_broadcast.append((self.timestamp[remetente], remetente, mensagem))

        self.mutex_broadcast_ordem_total.release()  # Libera o bloqueio mutex_broadcast_ordem_total

        self.mutex_broadcast_ordem_total.acquire()  # Adquire o bloqueio mutex_broadcast_ordem_total

        self.buffer_broadcast.sort(key=lambda x: x[0])  # Ordenar pelo timestamp

        while len(self.buffer_broadcast) > 0:
            timestamp, remetente, mensagem = self.buffer_broadcast.pop(0)
            self.delayed_print(f"Entrega broadcast em ordem total: {remetente} -> {mensagem}")
            for replica in self.replicas:
                if replica != self.replicas[remetente]:
                    self.repassar_mensagem(remetente, mensagem)
            self.entregar_mensagens_unicast(remetente)

        self.mutex_broadcast_ordem_total.release()  # Libera o bloqueio mutex_broadcast_ordem_total

        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")

    def conectar_replicas(self):
        for i in range(len(self.timestamp)):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('localhost', 6000 + i))
            sock.listen(1)
            self.replicas.append(sock)
    def repassar_mensagem(self, remetente, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.mutex_broadcast.acquire()  # Adquire o bloqueio mutex_broadcast
        self.queue_broadcast.put((remetente, mensagem))  # Insere a mensagem na fila
        self.mutex_broadcast.release()  # Libera o bloqueio mutex_broadcast
        self.entregar_mensagens_broadcast()  # Chamada para entregar as mensagens broadcast


    def enviar_mensagem_unicast(self, remetente, destinatario, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.queue_unicast.put((remetente, destinatario, mensagem))  # Insere a mensagem na fila
        self.entregar_mensagens_unicast(destinatario)  # Chamada para entregar as mensagens unicast

    def enviar_mensagem_broadcast(self, remetente, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.mutex_broadcast.acquire()  # Adquire o bloqueio mutex_broadcast
        self.queue_broadcast.put((remetente, mensagem))  # Insere a mensagem na fila
        self.mutex_broadcast.release()  # Libera o bloqueio mutex_broadcast
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
        self.mutex_broadcast.acquire()  # Adquire o bloqueio mutex_broadcast
        # print("1")

        while not self.queue_broadcast.empty():
            # print("2")

            remetente, mensagem = self.queue_broadcast.get()
            # print("3")

            self.buffer_broadcast.append((self.timestamp[remetente], remetente, mensagem))
            # print("4")

        self.buffer_broadcast.sort(key=lambda x: x[0])  # Ordenar pelo timestamp
        # print("5")
        while len(self.buffer_broadcast) > 0:
            # print("6")
            timestamp, remetente, mensagem = self.buffer_broadcast.pop(0)
            # print("7")
            self.delayed_print(f"Entrega broadcast: {remetente} -> {mensagem}")
            # print("8")
            for replica in self.replicas:
                # print("9")
                if replica != self.replicas[remetente]:
                    # print("repassando")
                    # self.mutex_broadcast.release()  # Libera o bloqueio mutex_broadcast

                    self.repassar_mensagem(remetente, mensagem)  # Repassa a mensagem para as demais réplicas
                    # print("repassando2")

        # print("deleyou")
        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")
        # print("liberou")
        self.mutex_broadcast.release()  # Libera o bloqueio mutex_broadcast

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
    def bateria_teste_unicast(self, remetente, destinatario):
        for i in range(3):
            mensagem = f"Mensagem {i + 1} de {remetente} para {destinatario}"
            self.enviar_mensagem_unicast(remetente, destinatario, mensagem)
            time.sleep(random.uniform(0.1, 0.5))

    # Função para executar uma bateria de testes de mensagens broadcast
    def bateria_teste_broadcast(self, remetente):
        for i in range(3):
            mensagem = f"Mensagem {i + 1} de {remetente} (broadcast)"
            self.enviar_mensagem_broadcast(remetente, mensagem)
            time.sleep(random.uniform(0.1, 0.5))


# Classe para o serviço de replicação ativa
class ServicoReplicaAtiva:
    def __init__(self, sistema_entrega):
        self.sistema_entrega = sistema_entrega

    def bateria_teste_broadcast_ordem_total(self, remetente):
        for i in range(3):
            mensagem = f"Mensagem {i + 1} de {remetente} (broadcast ordem total)"
            self.enviar_mensagem_broadcast_ordem_total(remetente, mensagem)
            time.sleep(random.uniform(0.1, 0.5))

    def enviar_mensagem_broadcast_ordem_total(self, remetente, mensagem):
        self.sistema_entrega.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.sistema_entrega.mutex_broadcast.acquire()  # Adquire o bloqueio mutex_broadcast
        self.sistema_entrega.queue_broadcast.put((remetente, mensagem))  # Insere a mensagem na fila
        self.sistema_entrega.mutex_broadcast.release()  # Libera o bloqueio mutex_broadcast
        self.sistema_entrega.entregar_mensagens_broadcast_ordem_total()  # Chamada para entregar as mensagens broadcast em ordem total





class ServicoReplicacaoPassiva:
    def __init__(self, sistema_entrega):
        self.sistema_entrega = sistema_entrega


    def bateria_teste_unicast(self, remetente, destinatario):
        for i in range(3):
            mensagem = f"Mensagem {i + 1} de {remetente} para {destinatario}"
            self.sistema_entrega.enviar_mensagem_unicast(remetente, destinatario, mensagem)
            time.sleep(random.uniform(0.1, 0.5))

    def bateria_teste_broadcast(self, remetente):
        for i in range(3):
            mensagem = f"Mensagem {i + 1} de {remetente} (broadcast)"
            self.sistema_entrega.enviar_mensagem_broadcast(remetente, mensagem)
            time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    sistema_entrega = SistemaEntrega(3)  # Cria o sistema de entrega com 3 réplicas
    sistema_entrega.conectar_replicas()  # Conecta as réplicas

    # Inicializa o serviço de replicação passiva sem líder fixo
    servico_replicacao_passiva = ServicoReplicacaoPassiva(sistema_entrega)

    # Executa uma bateria de testes para o serviço de replicação passiva
    t1 = Thread(target=servico_replicacao_passiva.bateria_teste_unicast, args=(0, 1))
    t2 = Thread(target=servico_replicacao_passiva.bateria_teste_unicast, args=(1, 0))
    t3 = Thread(target=servico_replicacao_passiva.bateria_teste_broadcast, args=(2,))
    t1.start()
    t2.start()
    t3.start()

    # Aguarda todas as threads terminarem
    t1.join()
    t2.join()
    t3.join()

    # # Inicializa o serviço de replicação ativa
    # servico_replica_ativa = ServicoReplicaAtiva(sistema_entrega)
    #
    # # Executa uma bateria de testes para o serviço de replicação ativa
    # t4 = Thread(target=servico_replica_ativa.bateria_teste_broadcast_ordem_total, args=(0,))
    # t5 = Thread(target=servico_replica_ativa.bateria_teste_broadcast_ordem_total, args=(1,))
    # t4.start()
    # t5.start()
    #
    # # Aguarda as threads terminarem
    # t4.join()
    # t5.join()

    sistema_entrega.encerrar_replicas()  # Encerra as réplicas

