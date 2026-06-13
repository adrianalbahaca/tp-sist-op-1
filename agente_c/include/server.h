#ifndef __SERVER_H__
#define __SERVER_H__

#include <sys/epoll.h>
#include <stddef.h>
#include <sys/types.h>
#include <pthread.h>

#define MAX_EVENTS 64
#define BUFF_SIZE 1024

#define UDP_DISCOVERY_PORT 12529
#define NUM_THREADS 4

/**
 * Identifica el rol de cada fd dentro del loop de epoll.
 * Para que event_loop sepa que handler ejecutar.
 */
typedef enum {
    CONN_TCP_PUBLIC_LISTEN,  // Escucha a otros agentes en C
    CONN_TCP_LOCAL_LISTEN,   // Escucha al proceso Erlang
    CONN_UDP_DISCOVERY,      // Recibe broadcasts UDP
    CONN_TCP_CLIENT_REMOTE,  // Conexión establecida con otro agente C
    CONN_TCP_CLIENT_LOCAL,   // Conexión establecida con Erlang
    CONN_TCP_OUTGOING        // Conexión saliente iniciada por este agente hacia otro nodo
} connection_type_t;

/* Encapsula el estado de una conexión y sus buffers. */
typedef struct {
    int fd;
    connection_type_t type;
    
    // Buffer para acumular lecturas parciales (soluciona el problema de EAGAIN)
    char read_buf[BUFF_SIZE];
    size_t read_pos;
    
    // Buffer para escrituras parciales
    // mutex, a implementar
    pthread_mutex_t write_mutex;
    char write_buf[BUFF_SIZE];
    size_t write_pos;
    size_t write_len;
} connection_t;

/**
 * Instancia epoll, levanta los 3 sockets de escucha (TCP público, TCP localhost 
 * y UDP broadcast). Los configura como no bloqueantes y los añade al monitor de 
 * epoll. 
 */
int init_server_sockets(const char* public_ip, int tcp_port, int *epfd);

/**
 * Toma el epfd. (Ver racecondition, deadlock, etc.)
 * Bucle bloqueante en epoll_wait. Se encarga de manejar cada conexión. Debería 
 * llamarse NUM_THREADS veces con pthread_create
 */
void *worker_thread_loop(void *arg); // int epfd

/**
 * Inicia una conexión TCP hacia otro nodo del clúster. Registra el fd en epoll 
 * con EPOLLOUT, esperando que la conexión tuvo éxito.
 * Cuando el planificador Erlang envía un Request, entonces se llamará a esta 
 * funcion para crear la conexión
 */
connection_t *connect_remote_node(int epfd, const char *ip, int port);

/**
 * Copia un mensaje (Como GRANTED) al buffer de escritura y el kernel dispara un 
 * EPOLLOUT. Entonces, algúno de los hilos tomará el mensaje y lo redireccionará.
 * Thread-safe, utilizar para programar envío de mensajes hacia la red.
 */
void enqueue_write(int epfd, connection_t *conn, const char *msg);

/**
 * Función a llamar cada N segundos para avisar de nuestra existencia a otros 
 * nodos
 * send_udp_broadcast(12529, "ANNOUNCE 192.168.1.10 8100 cpu:4 mem:8192 gpu:1\n")
 */
void send_udp_broadcast(int port, const char *message);

#endif /* __SERVER_H__ */