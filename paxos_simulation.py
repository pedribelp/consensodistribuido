"""
Simulación simplificada del algoritmo PAXOS (single-decree) para consenso distribuido.

Roles simulados:
  - Proposer: propone un valor y coordina las fases Prepare y Accept.
  - Acceptor: nodo que vota promesas (Fase 1) y aceptaciones (Fase 2). Aquí usamos
    3 nodos acceptors, que son además los que "aprenden" (Learner) el valor decidido.

Requisitos cubiertos por este prototipo:
  - >= 3 nodos simulados (Acceptor 1, 2, 3).
  - Fase 1 (Prepare/Promise) y Fase 2 (Accept/Accepted) entre proposer y acceptors.
  - Simulación de fallo de un nodo (un acceptor deja de responder) y verificación
    de que el protocolo sigue alcanzando consenso con la mayoría restante.

Ejecución:
    python3 paxos_simulation.py
"""

import time


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


class Acceptor:
    """Nodo Acceptor: responde a PREPARE y ACCEPT, y recuerda lo que ya prometió/aceptó."""

    def __init__(self, node_id):
        self.id = node_id
        self.alive = True
        self.promised_id = None     # mayor número de propuesta que ha prometido
        self.accepted_id = None     # número de la última propuesta aceptada
        self.accepted_value = None  # valor de la última propuesta aceptada

    def prepare(self, proposal_id):
        """Fase 1b: responde PROMISE o rechaza un PREPARE(n)."""
        if not self.alive:
            return None  # nodo caído: no responde (se simula timeout de red)
        if self.promised_id is None or proposal_id > self.promised_id:
            self.promised_id = proposal_id
            extra = f" (ya había aceptado {self.accepted_value})" if self.accepted_value else ""
            log(f"  Acceptor {self.id}: PROMISE a prepare({proposal_id}){extra}")
            return (True, self.accepted_id, self.accepted_value)
        log(f"  Acceptor {self.id}: RECHAZA prepare({proposal_id}); ya prometió {self.promised_id}")
        return (False, None, None)

    def accept(self, proposal_id, value):
        """Fase 2b: responde ACCEPTED o rechaza un ACCEPT(n, value)."""
        if not self.alive:
            return False
        if self.promised_id is None or proposal_id >= self.promised_id:
            self.promised_id = proposal_id
            self.accepted_id = proposal_id
            self.accepted_value = value
            log(f"  Acceptor {self.id}: ACCEPTED propuesta {proposal_id} -> valor='{value}'")
            return True
        log(f"  Acceptor {self.id}: RECHAZA accept({proposal_id}); ya prometió {self.promised_id}")
        return False


class Proposer:
    """Nodo Proposer: coordina las fases Prepare/Accept para lograr consenso sobre un valor."""

    def __init__(self, proposer_id, acceptors):
        self.id = proposer_id
        self.acceptors = acceptors
        self.round = 0

    def _next_proposal_id(self):
        # Número de propuesta único y globalmente creciente: ronda*100 + id del proposer
        self.round += 1
        return self.round * 100 + self.id

    def propose(self, value):
        proposal_id = self._next_proposal_id()
        majority = len(self.acceptors) // 2 + 1
        log(f"Proposer {self.id}: FASE 1 - envía PREPARE({proposal_id}) para el valor propuesto '{value}'")

        # ---------- Fase 1: Prepare / Promise ----------
        promises = []
        for acc in self.acceptors:
            result = acc.prepare(proposal_id)
            if result and result[0]:
                promises.append((acc, result[1], result[2]))

        if len(promises) < majority:
            log(f"Proposer {self.id}: sin mayoría en PREPARE ({len(promises)}/{len(self.acceptors)}); "
                f"propuesta abortada")
            return None

        log(f"Proposer {self.id}: mayoría de PROMISE alcanzada ({len(promises)}/{len(self.acceptors)})")

        # Regla de seguridad de Paxos: si algún acceptor ya había aceptado un valor
        # previo, el proposer debe re-proponer ESE valor en lugar del suyo propio.
        highest_accepted_id = None
        chosen_value = value
        for _, acc_id, acc_val in promises:
            if acc_id is not None and (highest_accepted_id is None or acc_id > highest_accepted_id):
                highest_accepted_id = acc_id
                chosen_value = acc_val

        if highest_accepted_id is not None:
            log(f"Proposer {self.id}: respeta el valor ya aceptado previamente '{chosen_value}' "
                f"(regla de seguridad de Paxos)")

        # ---------- Fase 2: Accept / Accepted ----------
        log(f"Proposer {self.id}: FASE 2 - envía ACCEPT({proposal_id}, '{chosen_value}')")
        accepted_count = 0
        for acc, _, _ in promises:
            if acc.accept(proposal_id, chosen_value):
                accepted_count += 1

        if accepted_count >= majority:
            log(f"Proposer {self.id}: *** CONSENSO ALCANZADO -> valor decidido = '{chosen_value}' *** "
                f"({accepted_count}/{len(self.acceptors)} aceptaciones)")
            return chosen_value
        else:
            log(f"Proposer {self.id}: sin mayoría en ACCEPT ({accepted_count}/{len(self.acceptors)}); "
                f"propuesta fallida")
            return None


def print_state(acceptors, title):
    log(f"--- {title} ---")
    for a in acceptors:
        status = "CAÍDO" if not a.alive else "activo"
        log(f"  Acceptor {a.id}: estado={status} promised={a.promised_id} "
            f"accepted_id={a.accepted_id} accepted_value={a.accepted_value}")


def main():
    log("=== SIMULACIÓN PAXOS: 3 nodos acceptors (N1, N2, N3) ===\n")
    acceptors = [Acceptor(i) for i in range(1, 4)]

    # Ronda 1: los 3 nodos están activos, se propone "A=1"
    log(">>> RONDA 1: los 3 nodos activos <<<")
    p1 = Proposer(1, acceptors)
    p1.propose("A=1")
    print_state(acceptors, "Estado tras la ronda 1")

    # Simulación de fallo: Acceptor 2 deja de responder
    log("\n>>> SIMULANDO FALLO: Acceptor 2 deja de responder <<<")
    acceptors[1].alive = False

    # Ronda 2: se intenta un nuevo consenso con un nodo caído (solo mayoría N1, N3)
    log("\n>>> RONDA 2: Acceptor 2 caído, se intenta consenso con la mayoría restante <<<")
    p2 = Proposer(2, acceptors)
    decided = p2.propose("B=2")
    print_state(acceptors, "Estado tras la ronda 2 (con fallo)")

    log("")
    if decided:
        log(f"CONCLUSIÓN: Paxos siguió funcionando pese a la caída de un nodo. "
            f"Valor final acordado por la mayoría = '{decided}'.")
        log("Nótese que, por la regla de seguridad de Paxos, el valor final coincide con")
        log("el ya aceptado en la ronda 1 ('A=1'): el protocolo nunca decide dos valores distintos.")
    else:
        log("El protocolo no alcanzó consenso en la ronda 2.")


if __name__ == "__main__":
    main()
