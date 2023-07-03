import random
import time
import threading

class SistemaEntrega:
    def __init__(self, num_processos):
        self.timestamp = [0] * num_processos
        self.buffer_unicast = {}
        self.buffer_broadcast = []

    def enviar_mensagem_unicast(self, remetente, destinatario, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        if destinatario not in self.buffer_unicast:
            self.buffer_unicast[destinatario] = []
        self.buffer_unicast[destinatario].append((self.timestamp[remetente], remetente, mensagem))
        self.entregar_mensagens_unicast(destinatario)

    def enviar_mensagem_broadcast(self, remetente, mensagem):
        self.timestamp[remetente] += 1  # Atualiza o relógio lógico do remetente
        self.buffer_broadcast.append((self.timestamp[remetente], remetente, mensagem))
        self.entregar_mensagens_broadcast()

    def entregar_mensagens_unicast(self, destinatario):
        if destinatario in self.buffer_unicast:
            mensagens = self.buffer_unicast[destinatario]
            mensagens.sort(key=lambda x: x[0])  # Ordenar pelo timestamp
            for timestamp, remetente, mensagem in mensagens:
                if all(self.timestamp[i] <= timestamp for i in range(len(self.timestamp))):
                    self.delayed_print(f"Entrega unicast para {destinatario}: {remetente} -> {mensagem}")
                    self.timestamp[destinatario] += 1  # Atualiza o relógio lógico do destinatário
            del self.buffer_unicast[destinatario]
        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")

    def entregar_mensagens_broadcast(self):
        self.buffer_broadcast.sort(key=lambda x: x[0])  # Ordenar pelo timestamp
        for timestamp, remetente, mensagem in self.buffer_broadcast:
            if all(self.timestamp[i] <= timestamp for i in range(len(self.timestamp))):
                self.delayed_print(f"Entrega broadcast: {remetente} -> {mensagem}")
                for i in range(len(self.timestamp)):
                    if i != remetente:  # Exclui a atualização do relógio lógico do remetente
                        self.timestamp[i] = max(self.timestamp[i],
                                                timestamp) + 1  # Atualiza o relógio lógico do destinatário
        self.buffer_broadcast = []
        self.delayed_print(f"Relógios lógicos atualizados: {self.timestamp}")

    def delayed_print(self, message):
        # Função auxiliar para adicionar um atraso aleatório e imprimir a mensagem
        delay = random.uniform(0.1, 0.5)  # Atraso aleatório entre 0.1 e 0.5 segundos
        time.sleep(delay)
        print(message)



# Seção de Teste 1: Mensagens Unicast
print("Seção de Teste 1: Mensagens Unicast")
sistema = SistemaEntrega(4)

sistema.enviar_mensagem_unicast(0, 1, "Mensagem 1")
sistema.enviar_mensagem_unicast(3, 2, "Mensagem 2")
sistema.enviar_mensagem_unicast(2, 0, "Mensagem 3")

# Ordem esperada das entregas unicast:
# Entrega unicast para 1: 0 -> Mensagem 1
# Entrega unicast para 2: 3 -> Mensagem 2
# Entrega unicast para 0: 2 -> Mensagem 3

# Seção de Teste 2: Mensagens Broadcast
print("Seção de Teste 2: Mensagens Broadcast")

sistema = SistemaEntrega(3)

sistema.enviar_mensagem_broadcast(0, "Broadcast 1")
sistema.enviar_mensagem_broadcast(1, "Broadcast 2")
sistema.enviar_mensagem_broadcast(2, "Broadcast 3")

# Ordem esperada das entregas broadcast:
# Entrega broadcast: 0 -> Broadcast 1
# Entrega broadcast: 1 -> Broadcast 2
# Entrega broadcast: 2 -> Broadcast 3

# Seção de Teste 3: Mensagens Unicast e Broadcast em Sequência
print("Seção de Teste 3: Mensagens Unicast e Broadcast em Sequência")

sistema = SistemaEntrega(3)

sistema.enviar_mensagem_unicast(1, 0, "Mensagem 1")
sistema.enviar_mensagem_unicast(0, 2, "Mensagem 2")
sistema.enviar_mensagem_broadcast(2, "Broadcast 1")
sistema.enviar_mensagem_broadcast(1, "Broadcast 2")

# Ordem esperada das entregas unicast:
# Entrega unicast para 0: 1 -> Mensagem 1
# Entrega unicast para 2: 0 -> Mensagem 2

# Ordem esperada das entregas broadcast:
# Entrega broadcast: 2 -> Broadcast 1
# Entrega broadcast: 1 -> Broadcast 2

# Seção de Teste 4: Mensagens Unicast e Broadcast Concorrentes
print("Seção de Teste 4: Mensagens Unicast e Broadcast Concorrentes")
sistema = SistemaEntrega(2)

sistema.enviar_mensagem_unicast(0, 1, "Mensagem 1")
sistema.enviar_mensagem_broadcast(1, "Broadcast 1")
sistema.enviar_mensagem_unicast(1, 0, "Mensagem 2")
sistema.enviar_mensagem_broadcast(0, "Broadcast 2")

# Ordem esperada das entregas unicast e multicast:
# Entrega unicast para 1: 0 -> Mensagem 1
# Entrega broadcast: 1 -> Broadcast 1
# Entrega unicast para 0: 1 -> Mensagem 2
# Entrega broadcast: 0 -> Broadcast 2
