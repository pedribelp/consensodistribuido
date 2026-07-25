# Prototipo de Consenso Distribuido: Raft y Paxos

Prototipos en Python que simulan los algoritmos de consenso **Raft** y **Paxos**
con 3 nodos, incluyendo elección/propuesta, replicación de un valor y
tolerancia a fallos (un nodo deja de responder y el protocolo sigue funcionando).

## Contenido

- `raft_simulation.py` — 3 nodos con hilos independientes. Elección de líder por
  timeout aleatorio y votación por mayoría, replicación del valor `A=1`,
  simulación de caída del líder y elección de un nuevo líder que sigue replicando (`B=2`).
- `paxos_simulation.py` — 3 nodos *acceptor* y proposers secuenciales. Fases
  *Prepare/Promise* y *Accept/Accepted* para acordar `A=1`, simulación de caída
  de un acceptor y verificación de que la mayoría restante mantiene el consenso.
- `logs_raft.txt` / `logs_paxos.txt` — salida real de una ejecución de cada script.
- `Informe_Consenso_Distribuido.pdf` — investigación, comparativa Paxos vs. Raft,
  explicación del código y análisis de los logs.

## Requisitos

- Python 3.8+ (sin dependencias externas)

## Cómo ejecutar

```bash
python3 raft_simulation.py
python3 paxos_simulation.py
```

Cada ejecución imprime en consola, con marca de tiempo, cada paso del protocolo:
solicitudes de voto, elección de líder, propuestas, replicación/aceptación de
valores, la caída simulada de un nodo y la recuperación del consenso.

## Resumen de lo que demuestra cada prototipo

**Raft:** un nodo gana la elección inicial por mayoría de votos y replica
`A=1` a los demás. Al marcar al líder como caído (`alive = False`), dejan de
llegar heartbeats; cuando expira el timeout de otro nodo, este inicia una
nueva elección, gana con el voto del nodo restante y continúa replicando
(`B=2`), confirmando la recuperación del consenso.

**Paxos:** un proposer completa Prepare/Promise y Accept/Accepted con los 3
acceptors para acordar `A=1`. Al marcar un acceptor como caído, un segundo
proposer repite ambas fases solo con la mayoría restante (2 de 3) y logra
consenso igualmente. Por la regla de seguridad de Paxos, respeta el valor ya
aceptado (`A=1`) en vez de imponer uno nuevo, mostrando que el protocolo nunca
decide dos valores distintos.

## Nota sobre la entrega completa

Este repositorio cubre el código y los logs de ejecución. El video explicativo
y la carga en Blackboard deben completarse por separado según los criterios
de entrega de la actividad.
