# HPC Resource Manager - Sistema de Gestión de Recursos Distribuidos

## Integrantes del equipo

| Rol | Nombre |
|-----|--------|
| Ingeniero de comunicaciones en C | Luciano Belardo |
| Gestor de recursos y estado | Adrian David Albahaca Chavez |
| Planificador y lógica anti-deadlock | Francisco Ares |
| Integración, pruebas e interoperabilidad | Genaro Lescano |

---

## Descripción del Proyecto

Este proyecto implementa un **middleware distribuido** para la gestión de recursos (CPUs, memoria, GPUs) en un clúster HPC simulado. El sistema consta de dos componentes principales:

- **Agente C**: Implementado en C con `epoll` y sockets no-bloqueantes. Actúa como servidor y cliente, gestionando recursos locales y comunicándose con otros agentes.
- **Planificador Erlang**: Implementado en Erlang, genera jobs, coordina la ejecución y aplica una estrategia anti-deadlock basada en ordenamiento por IP.

### Características principales

- Comunicación TCP no-bloqueante con `epoll`
- Descubrimiento dinámico de nodos mediante UDP broadcast
- Gestión de recursos locales con colas FIFO
- Prevención de deadlocks por ordenación de recursos
- Registro de eventos en archivo de log
- Interoperabilidad entre nodos heterogéneos

---
### Requisitos del Sistema

- **Para el Agente C**
GCC: Versión 9 o superior

- Bibliotecas: pthread, librerías estándar de C

- Sistema operativo: Distribución de Linux (con soporte para epoll)

- **Para el Planificador Erlang**
Erlang/OTP: Versión 24 o superior

---

### Compilación 
```sh
$ git clone https://github.com/adrianalbahaca/tp-sist-op-1/tree/main
$ make all
```

## Ejecución 
Para que los nodos puedan descubrirse entre si, debe usarse
la IP obtenible con: hostname -I

# Parámetros de ejecucion

- Direccion IP del nodo (hostname -I)
- Puerto TCP para comunicaciones 
- Cantidad de CPUs disponibles
- Cantidad de memoria en MB
- Cantidad de GPUs disponibles

# Ejecutar agente C - Sintaxis
```
make run-c IP=<IP_PUBLICA> PORT=<PUERTO> CPU=<CANTIDAD_CPU> MEM=<CANTIDAD_MEM> GPU=<CANTIDAD_GPU>
```

# Salida Esperada del agente C

```
[RX] De ERLANG LOCAL (fd 7) -> GET_NODES
[TX] A ERLANG LOCAL (fd 7) -> NODES 192.168.0.14:8000:cpu:4:mem:8192:gpu:1
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:4 | MEM: total: 8192, disp.:8192 | GPU: total: 1, disp.: 1 
=======================================
[RX] De ERLANG LOCAL (fd 7) -> JOB_REQUEST 9230 @192.168.0.14:cpu:1:mem:331:gpu:1
[TX] A ERLANG LOCAL (fd 7) -> JOB_GRANTED 9230
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:3 | MEM: total: 8192, disp.:7861 | GPU: total: 1, disp.: 0 
=======================================
[RX] De ERLANG LOCAL (fd 7) -> JOB_REQUEST 17422 @192.168.0.14:mem:507:gpu:1
[QUEUE]: [job_id: 17422, amount: 1, type: LOCAL -> NULL]
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:3 | MEM: total: 8192, disp.:7354 | GPU: total: 1, disp.: 0 
=======================================
[RX] De ERLANG LOCAL (fd 7) -> JOB_RELEASE 9230
[QUEUE]: [ NULL]
[DEQUEUE] Job 17422 desencolado (tipo 2, amount 1, available_restante 0)
[TX] A ERLANG LOCAL (fd 7) -> JOB_GRANTED 17422
[RELEASE] tipo 1: cola vacía, nada para desencolar (available=7685)
[RELEASE] tipo 0: cola vacía, nada para desencolar (available=4)
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:4 | MEM: total: 8192, disp.:7685 | GPU: total: 1, disp.: 0 
=======================================
[RX] De ERLANG LOCAL (fd 7) -> JOB_RELEASE 17422
[RELEASE] tipo 2: cola vacía, nada para desencolar (available=1)
[RELEASE] tipo 1: cola vacía, nada para desencolar (available=8192)
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:4 | MEM: total: 8192, disp.:8192 | GPU: total: 1, disp.: 1 
=======================================
[RX] De ERLANG LOCAL (fd 7) -> GET_NODES
[TX] A ERLANG LOCAL (fd 7) -> NODES 192.168.0.14:8000:cpu:4:mem:8192:gpu:1
========= STATUS DEL MANAGER =========
CPU: total: 4, disp.:4 | MEM: total: 8192, disp.:8192 | GPU: total: 1, disp.: 1 
=======================================
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
[QUEUE]: [ NULL]
...
```
# Ejecutar Planificador Erlang - Sintaxis
El planificador Erlang debe ejecutarse mientras el agente C está corriendo

```
make run-erl
```

# Salida Esperada del planificador Erlang
```
Conectado al agente C en el puerto 8000
Entrando a masterloop...
Job 1 esperando asignación...
Job 2 esperando asignación...
Job 1 trabajando...
Job 2 trabajando...
Job 1 finalizó. Solicitando RELEASE general.
Job 2 finalizó. Solicitando RELEASE general.
Job 3 esperando asignación...
Job 4 esperando asignación...
Job 3 trabajando...
Job 4 trabajando... 
...
```

## Limpieza
Para eliminar los binarios compilados
```
make clean
```