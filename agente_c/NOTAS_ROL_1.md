# Interfaz del Módulo de Comunicaciones (Capa de Red)

La capa de red procesa toda la E/S de forma asíncrona (epoll) y multihilo. Para mantener las responsabilidades separadas, la comunicación entre el motor de red y el Gestor de Estado se realiza estrictamente mediante los siguientes callbacks y funciones públicas.

**Regla de oro sobre la lectura:**
De la red les van a llegar **únicamente** mensajes completos y limpios, terminados en `\0` (carácter nulo de C). La capa de red oculta la fragmentación de TCP y elimina el salto de línea `\n` final antes de pasarles el string.

---

### HAY QUE PROGRAMAR (Callbacks)

La capa de red invocará estas funciones automáticamente. Ustedes deben definirlas en su módulo de estado.

- `extern void process_message(connection_t *conn, char *msg);`
  Se llama cada vez que llega un comando TCP completo (desde otro nodo o desde Erlang).
  *Responsabilidad:* Parsear `msg`. Si es un RESERVE y hay recursos locales, llaman a `enqueue_write(epfd, conn, "GRANTED...\n")`. Si no hay, guardan el puntero `conn` en su cola de pendientes para responderle más tarde.
  **Aclaración:** Traten al puntero `conn` como de solo lectura. Jamás le hagan `free()` ni `close()`.

- `extern void process_announce(const char *ip_sender, const char *message);`
  Se llama cada vez que el socket UDP intercepta un broadcast. 
  *Responsabilidad:* Leer el mensaje y actualizar la tabla de nodos conocidos y sus timestamps.

- `extern void process_disconnect(connection_t *conn);`
  Se llama justo antes de que la red destruya un socket (caída de nodo, desconexión de Erlang). 
  *Responsabilidad:* Buscar en la tabla de jobs activos o colas FIFO si hay operaciones ligadas a este puntero `conn` exacto, y liberar o abortar los recursos retenidos.

- `extern void process_connection_ready(connection_t *conn);`
  Se llama cuando un *handshake* TCP saliente hacia otro nodo se completó exitosamente.
  *Responsabilidad:* Buscar este `conn` en su registro de trabajos derivados y usar `enqueue_write` para mandar finalmente la petición al nodo remoto.

- `extern void process_connection_failed(connection_t *conn);`
  Se llama si un intento de conexión a otro nodo falló (ej. IP inaccesible o puerto cerrado).
  *Responsabilidad:* Abortar el intento de delegación de recursos y buscar un nodo alternativo en la tabla.

---

### PUEDEN USAR (API Pública)

Llamen a estas funciones cuando la lógica de estado necesite emitir datos hacia afuera.

- `void enqueue_write(int epfd, connection_t *conn, const char *msg);`
  Para enviar respuestas (`GRANTED`, `DENIED`, etc.) a un nodo específico.
  Pasen el `epfd` global, el puntero `conn` del cliente destino, y el string. 
  **IMPORTANTE:** Como TCP es un flujo de bytes, el string `msg` DEBE terminar explícitamente en `\n` (ej. `"GRANTED 105\n"`). La función es *thread-safe* (usa mutex interno); pueden llamarla desde cualquier hilo.

- `connection_t* connect_remote_node(int epfd, const char *ip, int port);`
  Para conectar hacia otro agente C cuando Erlang pide un recurso que no tenemos y decidimos derivarlo a `ip`.
  **Es no bloqueante.** La función retorna un puntero inmediatamente. Guarden ese puntero en la estructura de su tarea pendiente. La red les avisará mediante `process_connection_ready` cuando el canal esté abierto para empezar a escribir.

- `void send_udp_broadcast(int port, const char *message);`
  Inyecta un datagrama directo a la subred. El integrador debe usar esto en un hilo temporizador cada 5 segundos para anunciar el estado de la máquina.

---

### Secuencia de Arranque (Para el Integrador en `main.c`)

El arranque debe respetar estrictamente este orden para cumplir con los 2 segundos de espera silenciosa de UDP antes de procesar carga TCP:

```c
// 1. Levantar los sockets de escucha en epoll
init_server_sockets("127.0.1.1", 8100, &epfd_global);

// 2. Enviar nuestro primer broadcast al clúster para anunciar arranque
send_udp_broadcast(12529, "ANNOUNCE 127.0.1.1 8100 cpu:4 mem:8192 gpu:1\n");

// 3. Dormir el hilo principal 2 segundos. 
// El OS acumulará en su buffer los datagramas entrantes de los nodos vivos.
sleep(2);

// 4. Lanzar los hilos del pool. Procesarán todo el acumulado de golpe.
for(int i = 0; i < 4; i++) {
    pthread_create(&threads[i], NULL, worker_thread_loop, &epfd_global);
}

// 5. Lanzar el hilo auxiliar de broadcast periódico de recursos.