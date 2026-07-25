"""
Simulación simplificada del algoritmo RAFT para consenso distribuido.

Roles simulados:
  - FOLLOWER: nodo pasivo que espera mensajes del líder.
  - CANDIDATE: nodo que, tras un timeout de elección, pide votos para convertirse en líder.
  - LEADER: nodo que envía heartbeats y replica entradas de log a los seguidores.

Requisitos cubiertos por este prototipo:
  - >= 3 nodos simulados (N1, N2, N3), cada uno en su propio hilo.
  - Elección de líder mediante timeouts aleatorios y votación por mayoría.
  - Replicación de un valor simple (ej. "A=1") desde el líder hacia los seguidores.
  - Simulación de fallo del líder: se marca como caído y se verifica que el
    clúster elige un nuevo líder y sigue replicando valores (recuperación del consenso).

Ejecución:
    python3 raft_simulation.py
"""

import threading
import time
import random

FOLLOWER, CANDIDATE, LEADER = "FOLLOWER", "CANDIDATE", "LEADER"

PRINT_LOCK = threading.RLock()  # evita que los logs de distintos hilos se mezclen


def log(msg):
    with PRINT_LOCK:
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] {msg}")


class RaftNode:
    """Representa un nodo del clúster Raft."""

    def __init__(self, node_id, cluster):
        self.id = node_id
        self.cluster = cluster          # dict {id: RaftNode} -> simula la "red" del clúster
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log_entries = []           # entradas replicadas, ej. [("A", 1)]
        self.alive = True               # False simula un nodo caído (no responde a RPCs)
        self.election_timeout = self._new_timeout()
        self.last_heartbeat = time.time()
        self.lock = threading.RLock()
        self.stop_flag = False

    def _new_timeout(self):
        # Timeout aleatorio (escalado a segundos para observar el proceso en consola)
        return random.uniform(1.5, 3.0)

    # ---------------- RPCs simuladas ----------------
    def request_vote(self, term, candidate_id):
        """Un candidato solicita el voto de este nodo (Fase de elección)."""
        with self.lock:
            if not self.alive:
                return False  # nodo caído: no responde
            if term < self.current_term:
                return False
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = FOLLOWER
            if self.voted_for is None or self.voted_for == candidate_id:
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                return True
            return False

    def append_entries(self, term, leader_id, entries):
        """El líder envía heartbeat y/o replica entradas de log a este nodo."""
        with self.lock:
            if not self.alive:
                return False
            if term < self.current_term:
                return False
            self.current_term = term
            self.state = FOLLOWER
            self.voted_for = leader_id
            self.last_heartbeat = time.time()
            for e in entries:
                if e not in self.log_entries:
                    self.log_entries.append(e)
                    log(f"Nodo {self.id}: replicó entrada {e} del líder {leader_id} (term {term})")
            return True

    # ---------------- ciclo principal del nodo ----------------
    def run(self):
        while not self.stop_flag:
            if not self.alive:
                time.sleep(0.2)
                continue
            if self.state == LEADER:
                self._send_heartbeats()
                time.sleep(0.5)
            else:
                time.sleep(0.2)
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self._start_election()

    def _start_election(self):
        with self.lock:
            if not self.alive:
                return
            self.state = CANDIDATE
            self.current_term += 1
            term = self.current_term
            self.voted_for = self.id
            self.election_timeout = self._new_timeout()
            self.last_heartbeat = time.time()
        log(f"Nodo {self.id}: timeout de elección -> inicia elección para term {term}")

        votes = 1  # se auto-vota
        for nid, node in self.cluster.items():
            if nid == self.id:
                continue
            if node.alive and node.request_vote(term, self.id):
                votes += 1

        majority = len(self.cluster) // 2 + 1
        with self.lock:
            if self.state != CANDIDATE or self.current_term != term or not self.alive:
                return  # el estado cambió mientras se pedían votos
            if votes >= majority:
                self.state = LEADER
                log(f"Nodo {self.id}: elegido LÍDER en term {term} con {votes}/{len(self.cluster)} votos")
            else:
                self.state = FOLLOWER
                log(f"Nodo {self.id}: elección fallida en term {term} ({votes} votos, se requieren {majority})")

    def _send_heartbeats(self, entries=None):
        for nid, node in self.cluster.items():
            if nid == self.id:
                continue
            if node.alive:
                node.append_entries(self.current_term, self.id, entries or [])

    def propose(self, entry):
        """El 'cliente' pide al líder replicar un nuevo valor en el log."""
        with self.lock:
            if self.state != LEADER or not self.alive:
                return False
            if entry not in self.log_entries:
                self.log_entries.append(entry)
        log(f"Nodo {self.id} (LÍDER): propone y replica {entry} en term {self.current_term}")
        self._send_heartbeats(entries=[entry])
        return True


def get_leader(cluster):
    for node in cluster.values():
        if node.alive and node.state == LEADER:
            return node
    return None


def wait_for_leader(cluster, timeout=10, exclude_id=None):
    start = time.time()
    while time.time() - start < timeout:
        leader = get_leader(cluster)
        if leader and leader.id != exclude_id:
            return leader
        time.sleep(0.2)
    return None


def print_cluster_state(cluster, title):
    log(f"--- {title} ---")
    for node in cluster.values():
        status = "CAÍDO" if not node.alive else node.state
        log(f"  Nodo {node.id}: estado={status} term={node.current_term} log={node.log_entries}")


def main():
    log("=== SIMULACIÓN RAFT: 3 nodos (N1, N2, N3) ===\n")
    cluster = {}
    for i in range(1, 4):
        cluster[i] = RaftNode(i, cluster)

    threads = [threading.Thread(target=n.run, daemon=True) for n in cluster.values()]
    for t in threads:
        t.start()

    # 1. Elección inicial del líder
    leader = wait_for_leader(cluster)
    if not leader:
        log("No se pudo elegir un líder a tiempo.")
        return
    print_cluster_state(cluster, "Estado tras la elección inicial")

    # 2. Replicación de un valor simple: A=1
    time.sleep(0.5)
    leader.propose(("A", 1))
    time.sleep(1.5)
    print_cluster_state(cluster, "Estado tras replicar A=1")

    # 3. Simulación de fallo del líder
    log(f"\n>>> SIMULANDO FALLO: el nodo líder {leader.id} deja de responder <<<\n")
    failed_leader_id = leader.id
    leader.alive = False

    # 4. Nueva elección entre los nodos restantes
    new_leader = wait_for_leader(cluster, timeout=12, exclude_id=failed_leader_id)

    if new_leader:
        log(f"\nNuevo líder tras el fallo: Nodo {new_leader.id}")
        print_cluster_state(cluster, "Estado tras recuperación (nuevo líder)")
        new_leader.propose(("B", 2))
        time.sleep(1.5)
        print_cluster_state(cluster, "Estado final tras replicar B=2 con el nuevo líder")
        log("\nCONCLUSIÓN: el clúster RAFT toleró la caída del líder y recuperó el consenso.")
    else:
        log("El clúster no logró recuperar el consenso a tiempo.")

    for n in cluster.values():
        n.stop_flag = True
    time.sleep(0.3)


if __name__ == "__main__":
    main()
