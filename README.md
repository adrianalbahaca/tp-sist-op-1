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

Bibliotecas: pthread, librerías estándar de C

Sistema operativo: Linux (con soporte para epoll)

- **Para el Planificador Erlang**
Erlang/OTP: Versión 24 o superior

---

### Compilacion 
```sh
$ git clone https://github.com/adrianalbahaca/tp-sist-op-1/tree/main
$ make all
```

## Ejecucion 
para que los nodos puedan descubrirse entre si, deben usar
la IP de la red local, puedes obtenerla con: hostname -I

- # Parametros de ejecucion

- direccion de IP publica del nodo
- puerto TCP para comunicaciones 
- cantidad de CPUs disponibles
- cantidad de memoria en MB
- cantidad de GPUs disponibles

# Ejecutar agente C - Sintaxis
make run-c IP=<IP_PUBLICA> PORT=<PUERTO> CPU=<CANTIDAD_CPU> MEM=<CANTIDAD_MEM> GPU=<CANTIDAD_GPU>

# Salida Esperada del agente C
[RX] De ERLANG LOCAL (fd 5) -> GET_NODES
[TX] A ERLANG LOCAL (fd 5) -> NODES 192.168.1.50:8000:cpu:8:mem:16384:gpu:2;
[RX] De ERLANG LOCAL (fd 5) -> JOB_REQUEST 1 @192.168.1.50:cpu:2:mem:1024:gpu:0
[TX] A AGENTE REMOTO 192.168.1.51 (fd 7) -> RESERVE 1 gpu 1
...

# Ejecutar Planificador erlang - Sintaxis
el planificador Erlang debe ejecutarse despues de que el agente C este corriendo

make run-erl

# Salida Esperada del planificador Erlang
Conectado al agente C en el puerto 8000
Mensaje enviado: GET_NODES
Entrando a masterloop...
ESPERANDO
Job 1 trabajando...
Job 1 finalizó. Solicitando RELEASE general.
ESPERANDO
Job 2 trabajando...
...

## Limpieza
para eliminar los binarios compilados

make clean