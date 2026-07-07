# Estructuras del Resource Manager

## Estructuras activas (nuevo diseño)

---

### `TablaJobs` + `Job`
**Archivo:** `tabla_jobs.h` / `tabla_jobs.c`

**Propósito:** registro central de todos los jobs activos en este nodo, incluyendo los .

**Ciclo de vida de un job:**
1. Se crea cuando llega un `JOB_REQUEST` de Erlang local
2. Se actualiza cuando llegan `GRANTED` remotos (mueve de `pendientes` a `confirmadas`)
3. Se elimina cuando llega el `JOB_RELEASE` correspondiente

**Campos de `Job`:**
- `job_id` — identificador único del job
- `conn` — conexión de Erlang local que originó el `JOB_REQUEST` (a quién responderle el `JOB_GRANTED` o `JOB_DENIED`)
- `confirmadas` (lista de `Allocation`) — recursos ya reservados con `GRANTED` recibido. Se usa al momento del `JOB_RELEASE` para saber qué liberar
- `pendientes` (lista de `OutRequest`) — recursos remotos pedidos pero sin `GRANTED` todavía. Se vacía a medida que llegan los `GRANTED`

**Cuándo `pendientes` queda vacío:** todos los recursos remotos fueron confirmados → mandar `JOB_GRANTED` a `conn`

---

### `OutRequest`
**Archivo:** `tabla_jobs.h`

**Propósito:** representa un `RESERVE` mandado a un nodo remoto que todavía no fue confirmado.

**Campos:**
- `ip` — IP del nodo remoto al que se le mandó el `RESERVE`
- `tipo` + `amount` — qué recurso y cuánto se pidió
- `msg` — el mensaje ya formateado (`"RESERVE job_id tipo amount\n"`) para mandarlo cuando la conexión esté lista

**Cuándo se crea:** segunda pasada del `JOB_REQUEST`, para cada recurso remoto
**Cuándo se elimina:** cuando llega el `GRANTED` correspondiente de ese nodo

---

### `Allocation`
**Archivo:** `tabla_jobs.h`

**Propósito:** registro de un recurso ya confirmado (con `GRANTED` recibido) para un job.

**Campos:**
- `tipo` + `amount` — qué recurso y cuánto se reservó

**Cuándo se crea:** cuando llega un `GRANTED` (se mueve el `OutRequest` correspondiente a esta lista)
**Cuándo se usa:** al procesar `JOB_RELEASE` para saber exactamente qué liberar con `release_resource`

---

### `ColaPendingRequest` (por cada tipo de recurso)
**Archivo:** `pending_request.h`

**Propósito:** cola FIFO de jobs que están esperando un recurso específico (cpu, mem, o gpu) porque no había disponibilidad cuando lo pidieron.

**Campos clave:**
- `total_solicitado` — suma de todos los `amount` encolados actualmente. Se usa para el **control de admisión**: si `total_solicitado + nuevo_amount > total_recurso`, se rechaza con `DENIED` en vez de encolar
- `top` — frente de la cola (el siguiente en ser atendido)

**Garantía del control de admisión:** todo lo que entra a la cola **tiene garantizado** que va a poder satisfacerse eventualmente. No hay jobs encolados que nunca se resolverán.

**Cuándo se desencola:** en `release_resource`, cuando se libera suficiente cantidad del recurso

---

### `tabla_conns`
**Archivo:** `tabla_conns.h`

**Propósito:** caché de conexiones TCP salientes activas hacia otros agentes remotos, indexadas por IP.

**Cuándo se inserta:** en `process_connection_ready`, cuando una conexión asíncrona completó exitosamente
**Cuándo se elimina:** en `process_disconnect`, cuando una conexión se cierra
**Cuándo se usa:** en la segunda pasada del `JOB_REQUEST` y en el broadcast del `JOB_RELEASE`, para encontrar la conexión activa hacia un nodo remoto por su IP

---

### `tabla_nodos` (TablaNodos)
**Archivo:** `tabla_node_entry.h`

**Propósito:** directorio de nodos conocidos en la red, actualizado por los mensajes `ANNOUNCE` UDP periódicos.

**Campos de `NodeEntry`:**
- `ip` + `puerto` — dirección del nodo
- `cpu_disp`, `mem_disp`, `gpu_disp` — recursos disponibles según el último anuncio
- `last_seen` — timestamp del último `ANNOUNCE` recibido (para expirar nodos muertos)

**Cuándo se usa:** en la segunda pasada del `JOB_REQUEST`, para obtener el puerto de un nodo remoto antes de llamar a `connect_remote_node`

---

## Estructuras eliminadas (diseño anterior)

Estas estructuras existían en el diseño anterior y fueron reemplazadas por las de arriba:

- **`job_owners`** → reemplazada por `TablaJobs` (el campo `conn` de cada `Job`)
- **`pendientes_salientes`** → reemplazada por la lista `pendientes` de `OutRequest` dentro de cada `Job`
- **`OutReq`** (struct local en `resource_manager.c`) → reemplazada por `OutRequest` en `tabla_jobs.h`

---

## Flujos principales

### JOB_REQUEST
1. Primera pasada (locales): `reserve_resource` → si `RM_DENIED`, rollback + `JOB_DENIED`; si `RM_GRANTED`/`RM_QUEUED`, `tabla_jobs_insert`
2. Segunda pasada (remotos, solo si primera pasada exitosa): para cada recurso remoto, si hay conexión en `tabla_conns` → mandar `RESERVE` directo; si no → `connect_remote_node` (asíncrono); en ambos casos `tabla_jobs_insert` con el `OutRequest`

### GRANTED (de nodo remoto)
1. Buscar job en `tabla_jobs` por `job_id`
2. Sacar el `OutRequest` correspondiente de `pendientes` → mover a `confirmadas`
3. Si `pendientes` vacío → mandar `JOB_GRANTED` a `job->conn`

### JOB_RELEASE (de Erlang local)
1. `tabla_jobs_remove` → libera todos los recursos de `confirmadas` vía `release_resource`
2. Broadcast a nodos remotos para que liberen también

### process_connection_ready
1. `tabla_conns_insert(ip, conn)`
2. Buscar en `tabla_jobs` jobs con `OutRequest` pendiente para esa IP
3. Mandar el `msg` del `OutRequest` con `enqueue_write`

### process_connection_failed
1. Buscar en `tabla_jobs` jobs con `OutRequest` pendiente para esa IP
2. Para cada uno: rollback local (`queue_delete_by_job_id` en las 3 colas) + `tabla_jobs_remove` + `JOB_DENIED` a `job->conn`
