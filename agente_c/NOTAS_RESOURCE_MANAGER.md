# Estado de resource_manager.c — domingo noche

## Qué quedó funcionando
- Protocolo entre agentes completo: RESERVE, RELEASE, GRANTED, DENIED
- JOB_REQUEST: soporta recursos locales y remotos
- Si el recurso es remoto, busca conexión activa (tabla_conns) o conecta de cero
  con connect_remote_node, usando el puerto de tabla_nodos (poblada por ANNOUNCE)
- process_disconnect: libera todo lo asociado a una conexión que se cae
  (colas pendientes, jobs con recursos concedidos, entrada en tabla_conns)
- GRANTED/DENIED que llegan de un nodo remoto se reenvían al conn que originó
  el JOB_REQUEST (tabla job_owners)

## Qué falta / qué revisar
- GET_NODES (que Erlang pide para listar nodos) — no está implementado todavía
- timerfd / timeouts en reservas pendientes — no implementado (es opcional según el TP)
- tabla_jobs_remove tiene una variable sin usar (warning), revisar si hace falta
  para liberar un job específico por job_id (sin desconexión)
- Repasar si hace falta loguear concesiones/denegaciones en archivo (eso lo
  pide el enunciado para el lado de Erlang, no para C, pero conviene confirmar)

## Para correr/probar
- make desde la raíz del repo (Makefile ya apunta a agente_c/src y agente_c/include)
- Si agregan algo a resource_manager.c, mutex existentes:
  - manager.recursos[tipo].mutex (uno por recurso: cpu/mem/gpu)
  - tabla_conns.mutex, tabla_nodos.mutex
  - mutex_pendientes_salientes, mutex_job_owners
  - Orden de adquisición usado: TablaJobs -> Recurso (nunca al revés). 
    Si agregan código nuevo que toque varios mutex a la vez, mantener ese orden
    para evitar deadlocks. 