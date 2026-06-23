#include "../include/server.h"
#include "../include/resource_manager.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <pthread.h>

/**
 * O_NONBLOCK para que retorne automáticamente y no se bloquee al hacer read, 
 * write o accept (EAGAIN o EWOULDBLOCK)
 */
static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        exit(EXIT_FAILURE);
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL");
        exit(EXIT_FAILURE);
    }
}

/* Crea un socket, lo bindea, lo pone en listen (si es TCP) y lo agrega a epoll */
static connection_t* create_listen_socket(int epfd, const char* ip, int port, connection_type_t type, int is_udp) {
    // Crea fd de INET (No local), y setea tcp o udp según vaya
    int fd = socket(AF_INET, is_udp ? SOCK_DGRAM : SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // SO_REUSEADDR evita el error "Address already in use" al reiniciar el servidor rápidamente
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr)); // inicializa a 0
    addr.sin_family = AF_INET; // Por INET
    addr.sin_port = htons(port); 
    inet_pton(AF_INET, ip, &addr.sin_addr); // Escucharé a esta IP

    // Vincula el socket a esta IP 
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }

    // Solo los sockets TCP requieren listen()
    if (!is_udp) {
        // Lo pone en estado de escucha pasiva para recibir peticiones
        if (listen(fd, SOMAXCONN) < 0) {
            perror("listen");
            exit(EXIT_FAILURE);
        }
    }

    // Para que accept no bloquee el hilo
    set_nonblocking(fd);

    // Alocamos la estructura en el heap para que persista
    connection_t *conn = malloc(sizeof(connection_t));
    if (!conn) {
        perror("malloc");
        close(fd);
        exit(EXIT_FAILURE);
    }

    conn->fd = fd;
    conn->type = type;
    conn->read_pos = 0;
    pthread_mutex_init(&conn->write_mutex, NULL);
    conn->write_pos = 0;
    conn->write_len = 0;

    // Registramos el evento de lectura (EPOLLIN) asociando el puntero de la estructura
    // EPOLLEXCLUSIVE es para que solo lo tome 1 del epoll_wait, como estamos en multihilo
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLEXCLUSIVE; 
    ev.data.ptr = conn;
    
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        perror("epoll_ctl");
        close(fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        exit(EXIT_FAILURE);
    }

    return conn;
}

/* Inicia una conexión TCP saliente hacia un nodo remoto de forma asíncrona */
connection_t *connect_remote_node(int epfd, const char *ip, int port) {
    // Crea el socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket saliente");
        return NULL;
    }

    set_nonblocking(sockfd);

    struct sockaddr_in remote_addr;
    memset(&remote_addr, 0, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &remote_addr.sin_addr);

    /**
     * connect() sobre un socket no bloqueante retornará inmediatamente.
     * Si la conexión es a localhost, puede retornar 0.
     * Para red externa, retornará -1 con errno == EINPROGRESS.
     * "Inicio del handshake" como cliente
     */
    int res = connect(sockfd, (struct sockaddr*)&remote_addr, sizeof(remote_addr));
    
    if (res < 0 && errno != EINPROGRESS) {
        perror("connect saliente");
        close(sockfd);
        return NULL;
    }

    connection_t *conn = malloc(sizeof(connection_t));
    if (!conn) {
        perror("malloc conn saliente");
        close(sockfd);
        return NULL;
    }

    conn->fd = sockfd;
    conn->type = CONN_TCP_OUTGOING;
    conn->read_pos = 0;
    pthread_mutex_init(&conn->write_mutex, NULL);
    conn->write_pos = 0;
    conn->write_len = 0;

    struct epoll_event ev;
    /**
     * Se espera EPOLLOUT para confirmar que el handshake finalizó. O sea, cuando
     * se pueda escribir significa que funcionó
     * EPOLLONESHOT garantiza exclusión mutua en el event loop multihilo.
     */
    ev.events = EPOLLOUT | EPOLLONESHOT;
    ev.data.ptr = conn;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev) == -1) {
        perror("epoll_ctl saliente");
        close(sockfd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return NULL;
    }

    return conn;
}

/* Evalúa el resultado de connect_remote_node, completando la conexión */
static void handle_outgoing_connection_event(int epfd, connection_t *conn) {
    int socket_error = 0;
    socklen_t errlen = sizeof(socket_error);

    // Obtiene el resultado final
    if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &socket_error, &errlen) < 0) {
        perror("getsockopt SO_ERROR");
        process_connection_failed(conn);
        close(conn->fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return;
    }
    
    if (socket_error != 0) {
        // La conexión asíncrona falló (ej. ECONNREFUSED, ETIMEDOUT)
        fprintf(stderr, "Fallo al conectar nodo remoto: %s\n", strerror(socket_error));
        // Aborta misión, descarta los paquetes
        process_connection_failed(conn);
        close(conn->fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return;
    }

    /**
     * La conexión fue exitosa. 
     * Se debe reconfigurar el evento en epoll para operar de forma regular
     * esperando lecturas (EPOLLIN) para recibir los GRANTED/DENIED.
     */
    // Ahora la conexión existe
    conn->type = CONN_TCP_CLIENT_REMOTE; 

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.ptr = conn;

    // Actualiza el registro existente, diciendo que ahora escuche, solo le interesa cuando reciba algo
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
        perror("epoll_ctl mod connection success");
        close(conn->fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return;
    }

    // Hace lo que pensabas hacer en un inicio con tu nueva conexión
    process_connection_ready(conn);
}

/* Consume los datagramas pendientes */
static void handle_udp_read(connection_t *conn) {
    char buffer[BUFF_SIZE];
    struct sockaddr_in sender_addr;
    socklen_t sender_len = sizeof(sender_addr);

    /**
     * Iteración estricta para procesar todos los datagramas encolados
     * en el buffer de recepción del sistema operativo.
     */
    while (1) {
        ssize_t bytes_read = recvfrom(conn->fd, buffer, BUFF_SIZE - 1, 0, 
                                      (struct sockaddr*)&sender_addr, &sender_len);

        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Buffer vacío. Fin del procesamiento.
                break;
            } else {
                perror("recvfrom UDP error");
                break;
            }
        }

        buffer[bytes_read] = '\0'; // Terminación nula para procesar como string C

        // Extracción de la IP de origen para mantener la tabla de nodos
        char sender_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(sender_addr.sin_addr), sender_ip, INET_ADDRSTRLEN);

        // Se delega el parseo del formato "ANNOUNCE <puerto> <recursos>"
        process_announce(sender_ip, buffer);
    }

    /* Nota:
     * Al usar EPOLLEXCLUSIVE en lugar de EPOLLONESHOT para este socket,
     * no es necesario rearmarlo con epoll_ctl. El socket sigue escuchando
     * y el kernel gestionará los bloqueos correctamente.
     */
}

/* Lee los datos de la conexión TCP */
static int handle_tcp_read(int epfd, connection_t *conn) {
    int bytes_read = read(conn->fd, conn->read_buf + conn->read_pos, BUFF_SIZE - conn->read_pos - 1);

    if (bytes_read < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            pthread_mutex_lock(&conn->write_mutex);
            /** 
             * No hay más datos por leer en el buffer del socket del OS.
             * Debido a EPOLLONESHOT, es obligatorio reactivar el descriptor
             * para volver a recibir eventos de este cliente.
             */
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLONESHOT;
            ev.data.ptr = conn;
            if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
                perror("epoll_ctl mod EAGAIN");
                pthread_mutex_unlock(&conn->write_mutex);
                process_disconnect(conn);
                close(conn->fd);
                pthread_mutex_destroy(&conn->write_mutex);
                free(conn);
                return -1;
            }
            pthread_mutex_unlock(&conn->write_mutex);
            return 0;
        } else {
            // Error real de E/S
            perror("read error");
            process_disconnect(conn);
            close(conn->fd);
            pthread_mutex_destroy(&conn->write_mutex);
            free(conn);
            return -1;
        }
    }

    if (bytes_read == 0) {
        /* EOF: El extremo remoto cerró la conexión de manera ordenada */
        // NOTIFICACIÓN ANTES DE DESTRUIR
        process_disconnect(conn);
        close(conn->fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return -1;
    }

    // Se actualiza el puntero lógico del buffer
    conn->read_pos += bytes_read;
    conn->read_buf[conn->read_pos] = '\0'; 

    char *line_start = conn->read_buf;
    char *newline_pos;

    /** 
     * Extracción iterativa: TCP es un flujo de bytes, no de mensajes.
     * Un único read() puede retornar múltiples mensajes concatenados
     * (ej: "RESERVE 1001 cpu 1\nRELEASE 1002 mem 10\n").
     */
    while ((newline_pos = strchr(line_start, '\n')) != NULL) {
        *newline_pos = '\0'; // Mutar el '\n' para crear una string válida en C
        
        // Llamada a la capa de aplicación
        process_message(conn, line_start);

        line_start = newline_pos + 1;
    }

    /** 
     * Compactación del buffer: Si quedaron bytes de un mensaje incompleto
     * al final del buffer, se desplazan al inicio para concatenarlos
     * con la próxima lectura.
     */
    size_t remaining = (conn->read_buf + conn->read_pos) - line_start;
    if (remaining > 0 && line_start > conn->read_buf) {
        memmove(conn->read_buf, line_start, remaining);
    }
    conn->read_pos = remaining;

    /* Reactivación final del descriptor en epoll (porque era ONESHOT) */
    pthread_mutex_lock(&conn->write_mutex);
    struct epoll_event ev;
    if (conn->write_len > 0) {
        ev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
    } else {
        ev.events = EPOLLIN | EPOLLONESHOT;
    }
    
    ev.data.ptr = conn;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
        perror("epoll_ctl mod rearm");
        pthread_mutex_unlock(&conn->write_mutex);
        process_disconnect(conn);
        close(conn->fd);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
        return -1;
    }
    pthread_mutex_unlock(&conn->write_mutex);
    return 0;
}

/* Transmitir los datos por la red */
static void handle_tcp_write(int epfd, connection_t *conn) {
    pthread_mutex_lock(&conn->write_mutex);
    // Calcula los bytes restantes por enviar
    size_t pending_bytes = conn->write_len - conn->write_pos;

    if (pending_bytes == 0) {
        /* Salvaguarda lógica si epoll disparó un evento espurio */
        struct epoll_event ev = { .events = EPOLLIN | EPOLLONESHOT, .data.ptr = conn };
        epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev);
        pthread_mutex_unlock(&conn->write_mutex);
        return;
    }
    ssize_t bytes_written = write(conn->fd, conn->write_buf + conn->write_pos, pending_bytes);

    if (bytes_written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            /**
             * El buffer del socket está lleno. 
             * Se rearma el descriptor esperando EPOLLOUT para reanudar luego.
             */
            struct epoll_event ev;
            ev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
            ev.data.ptr = conn;
            if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
                perror("epoll_ctl mod EAGAIN write");
                close(conn->fd);
                pthread_mutex_unlock(&conn->write_mutex);
                pthread_mutex_destroy(&conn->write_mutex);
                free(conn);
                return;
            }
            pthread_mutex_unlock(&conn->write_mutex);
            return;
        } else {
            // Error de conexión (ej. EPIPE si el cliente cerró el socket prematuramente)
            perror("write error");
            close(conn->fd);
            pthread_mutex_unlock(&conn->write_mutex);
            pthread_mutex_destroy(&conn->write_mutex);
            free(conn);
            return;
        }
    }

    // Actualizamos el puntero de la cantidad de bytes enviados
    conn->write_pos += bytes_written;

    struct epoll_event ev;
    
    if (conn->write_pos == conn->write_len) {
        /* Buffer vaciado. 
         * Es imperativo eliminar EPOLLOUT de la máscara de eventos.
         * Si se mantiene EPOLLOUT sin tener datos pendientes, epoll_wait
         * retornará inmediatamente en un bucle infinito (busy-waiting)
         * porque el socket siempre está listo para escribir.
         */
        conn->write_pos = 0;
        conn->write_len = 0;
        ev.events = EPOLLIN | EPOLLONESHOT;
    } else {
        /* Aún quedan bytes por enviar. Se mantiene EPOLLOUT. */
        ev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
    }

    // Reactivo el fd
    ev.data.ptr = conn;
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
        perror("epoll_ctl mod write update");
        close(conn->fd);
        pthread_mutex_unlock(&conn->write_mutex);
        pthread_mutex_destroy(&conn->write_mutex);
        free(conn);
    }
    pthread_mutex_unlock(&conn->write_mutex);
}

/* Acepta todas las conexiones TCP entrantes en los listen sockets*/
static void handle_accept(int epfd, connection_t *listen_conn) {
    /** 
     * Se itera sobre accept() hasta que retorne EAGAIN para limipiar todo el buffer
     */
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(listen_conn->fd, (struct sockaddr *)&client_addr, &client_len);
        
        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Se vació la cola de conexiones pendientes
                break;
            } else {
                perror("accept");
                break;
            }
        }

        set_nonblocking(client_fd);

        connection_t *new_conn = malloc(sizeof(connection_t));
        if (!new_conn) {
            perror("malloc new_conn");
            close(client_fd);
            continue;
        }

        new_conn->fd = client_fd;
        new_conn->read_pos = 0;
        pthread_mutex_init(&new_conn->write_mutex, NULL);
        new_conn->write_pos = 0;
        new_conn->write_len = 0;

        // Derivación del tipo de conexión según el socket de origen
        if (listen_conn->type == CONN_TCP_PUBLIC_LISTEN) {
            new_conn->type = CONN_TCP_CLIENT_REMOTE;
        } else if (listen_conn->type == CONN_TCP_LOCAL_LISTEN) {
            new_conn->type = CONN_TCP_CLIENT_LOCAL;
        }

        struct epoll_event ev;
        /** 
         * EPOLLONESHOT es porque como estamos en multithread, si no se pone puede
         * que varios lo agarren y no queremos eso, una vez que laburo se debe reagregar.
         */
        ev.events = EPOLLIN | EPOLLONESHOT;
        ev.data.ptr = new_conn;

        if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
            perror("epoll_ctl add client");
            close(client_fd);
            pthread_mutex_destroy(&new_conn->write_mutex);
            free(new_conn);
        }
    }
}

// wait_for_clients - ish
void* worker_thread_loop(void* arg) {
    int epfd = *(int*)arg;
    struct epoll_event events[MAX_EVENTS];

    while (1) {
        // Bloquea el hilo hasta que haya actividad en algún file descriptor
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        
        if (nfds == -1) {
            // EINTR significa que epoll_wait fue interrumpido por una señal del sistema. Es seguro reintentar.
            if (errno == EINTR) continue; 
            
            perror("epoll_wait");
            pthread_exit(NULL);
        }

        for (int i = 0; i < nfds; i++) {
            connection_t *conn = (connection_t *)events[i].data.ptr;
            int connection_closed = 0;

            // Tratamiento de errores en el socket (cierre abrupto, reset)
            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & (EPOLLIN | EPOLLOUT)))) {
                fprintf(stderr, "Error en el socket %d\n", conn->fd);
                // NOTIFICACIÓN ANTES DE DESTRUIR
                process_disconnect(conn);
                close(conn->fd);
                pthread_mutex_destroy(&conn->write_mutex);
                free(conn);
                continue;
            }

            // Evento de lectura disponible
            if (events[i].events & EPOLLIN) {
                switch (conn->type) {
                    case CONN_TCP_PUBLIC_LISTEN:
                    case CONN_TCP_LOCAL_LISTEN:
                        // Nuevas conexiones entrantes
                        handle_accept(epfd, conn);
                        break;
                        
                    case CONN_UDP_DISCOVERY:
                        // Paquetes de descubrimiento de otros nodos
                        handle_udp_read(conn);
                        break;
                        
                    case CONN_TCP_CLIENT_REMOTE:
                    case CONN_TCP_CLIENT_LOCAL:
                    case CONN_TCP_OUTGOING:
                        if (handle_tcp_read(epfd, conn) == -1) {
                            connection_closed = 1;
                        }
                        break;
                }
            }

            // Evento de escritura disponible (el buffer del socket del OS tiene espacio)
            if (!connection_closed && events[i].events & EPOLLOUT) {
                switch (conn->type) {
                    case CONN_TCP_CLIENT_REMOTE:
                    case CONN_TCP_CLIENT_LOCAL:
                        handle_tcp_write(epfd, conn);
                        break;

                    case CONN_TCP_OUTGOING:
                        handle_outgoing_connection_event(epfd, conn);
                        break;

                    default:
                        break;
                }
            }
        }
    }
    return NULL;
}

/* Inicializa la estructura del agente */
int init_server_sockets(const char* public_ip, int tcp_port, int *epfd) {

    *epfd = epoll_create1(0);
    if (*epfd < 0) {
        perror("epoll_create1");
        return -1;
    }
    
    // TCP Público (Agentes C) -- Escuchará otros servidores
    create_listen_socket(*epfd, public_ip, tcp_port, CONN_TCP_PUBLIC_LISTEN, 0);
    
    // TCP Local (Erlang) -- Escuchará al Erland local
    create_listen_socket(*epfd, "127.0.0.1", tcp_port, CONN_TCP_LOCAL_LISTEN, 0);
    
    // UDP Broadcast (Descubrimiento) con 0.0.0.0 (INADDR_ANY)
    create_listen_socket(*epfd, "0.0.0.0", UDP_DISCOVERY_PORT, CONN_UDP_DISCOVERY, 1);

    /**
     * Después de estos 3 create, se han creado 3 sockets de escucha que ahora
     * iterarán en work_thread_loop. Están en la instancia de epoll y estarán
     * esperando que alguien les diga algo. Los 3 están en escucha por si les 
     * llega algo, y según cuando llegue un mensjae, el epoll verá para quien y
     * lo redirigirá
     */

    return 0;
}

void enqueue_write(int epfd, connection_t *conn, const char *msg) {
    size_t msg_len = strlen(msg);
     
    pthread_mutex_lock(&conn->write_mutex);

    // Verificación de capacidad (omitiendo realloc dinámico para simplificar)
    if (conn->write_len + msg_len >= BUFF_SIZE) {
        fprintf(stderr, "Error: Buffer de escritura lleno para fd %d\n", conn->fd);
        pthread_mutex_unlock(&conn->write_mutex);
        return;
    }

    // Copiar el nuevo mensaje al final real de la cola pendiente
    memcpy(conn->write_buf + conn->write_pos + conn->write_len, msg, msg_len);
    conn->write_len += msg_len;

    /* Forzar la activación de EPOLLOUT en el kernel.
     * Al agregar EPOLLOUT, el hilo despertará en la próxima iteración
     * de epoll_wait e invocará handle_tcp_write.
     */
    
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLONESHOT;
    ev.data.ptr = conn;
    
    if (epoll_ctl(epfd, EPOLL_CTL_MOD, conn->fd, &ev) == -1) {
        perror("epoll_ctl mod enqueue_write");
    }
    
    pthread_mutex_unlock(&conn->write_mutex);
}

/* Emite un mensaje broadcast udp */
void send_udp_broadcast(int port, const char *message) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket broadcast saliente");
        return;
    }

    /**
     * Modificación de la flag de socket a nivel kernel para permitir el envío 
     * de paquetes a direcciones de broadcast. Si no se habilita, sendto() fallará con EACCES.
     */
    int broadcast_enable = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &broadcast_enable, sizeof(broadcast_enable)) < 0) {
        perror("setsockopt SO_BROADCAST");
        close(sock);
        return;
    }

    struct sockaddr_in broadcast_addr;
    memset(&broadcast_addr, 0, sizeof(broadcast_addr));
    broadcast_addr.sin_family = AF_INET;
    broadcast_addr.sin_port = htons(port);
    // INADDR_BROADCAST es equivalente a 255.255.255.255
    broadcast_addr.sin_addr.s_addr = htonl(INADDR_BROADCAST); 

    // Envío del datagrama completo de una sola vez
    ssize_t sent = sendto(sock, message, strlen(message), 0, 
                          (struct sockaddr*)&broadcast_addr, sizeof(broadcast_addr));
    
    if (sent < 0) {
        perror("sendto broadcast");
    }

    // Se destruye el socket efímero; no se necesita mantener estado para enviar datagramas sueltos
    close(sock);
}