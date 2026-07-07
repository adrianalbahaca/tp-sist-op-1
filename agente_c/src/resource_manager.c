#include "resource_manager.h"
#include "server.h"
#include "parsing.h"
#include "pending_request.h"
#include "tabla_jobs.h"
#include "tabla_conns.h"
#include "resource_types.h"
#include "tabla_node_entry.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>

typedef enum {
    RM_GRANTED,
    RM_QUEUED,
    RM_DENIED
} result_t;

// La cola de requests pendientes se implementará con una lista simplemente enlazad

typedef struct {
    int available_amount;
    int total_amount;
    ColaPendingRequest cola;
    pthread_mutex_t mutex;
} Recurso;

typedef struct {
    Recurso recursos[3];
    TablaJobs tabla;
} ResourceManager;

static char* resource_type_to_str(resource_t type) {
    switch(type) {
        case RESOURCE_CPU:
            return "cpu";
            break;
        case RESOURCE_MEM:
            return "mem";
            break;
        case RESOURCE_GPU:
            return "gpu";
            break;
        default:
            return NULL;
            break;
    }
}

/**
 * El gestor de recursos se define estáticamente para trabajar con memoria dinámica lo menos posible y evitar complicaciones
 */
static ResourceManager manager;

/**
 * La tabla de conexiones se declara de forma estática acá
 */
static TablaConns conns;

/**
 * Se ajusta el epoll local para que lo use el manager
 */
static int g_epfd;

void manager_set_epoll(int epfd) {
    g_epfd = epfd;
}

static char g_ip[16];

void manager_set_ip(const char *ip) {
    strncpy(g_ip, ip, sizeof(g_ip) - 1);
    g_ip[sizeof(g_ip) - 1] = '\0';
    return;
}

 /**
 * =====================================================================================
 * Funciones de Recurso
 * =====================================================================================
 */

// Atiende una orden RESERVE y devuelve una respuesta acorde
static result_t reserve_resource (resource_t tipo, int amount, int job_id, connection_t *conn) {
    result_t result;
    pthread_mutex_lock(&manager.recursos[tipo].mutex);

    if (manager.recursos[tipo].available_amount >= amount) {
        manager.recursos[tipo].available_amount -= amount;
        result = RM_GRANTED;
    }
    else if (manager.recursos[tipo].total_amount < amount) {
        result = RM_DENIED;
    }
    else if (queue_enqueue(&manager.recursos[tipo].cola, job_id, amount, conn, manager.recursos[tipo].total_amount)) {
        result = RM_QUEUED;
    }
    else result = RM_DENIED;

    pthread_mutex_unlock(&manager.recursos[tipo].mutex);

    return result;
}

// Atiende una orden RELEASE, liberando la memoria y la cola acorde
void release_resource(resource_t tipo, int amount) {
    pthread_mutex_lock(&manager.recursos[tipo].mutex);
    manager.recursos[tipo].available_amount += amount;

    ColaPendingRequest *cola = &manager.recursos[tipo].cola;

    if (queue_is_empty(cola)) {
        printf("[RELEASE] tipo %d: cola vacía, nada para desencolar (available=%d)\n", tipo, manager.recursos[tipo].available_amount);
    } else if (manager.recursos[tipo].available_amount < cola->top->amount) {
        printf("[RELEASE] tipo %d: cola tiene job %d pidiendo %d, pero solo hay %d disponible\n",
               tipo, cola->top->job_id, cola->top->amount, manager.recursos[tipo].available_amount);
    }

    char msg[BUFF_SIZE];
    // Desencolamos los trabajos que ahora pueden satisfacerse
    while (!queue_is_empty(cola) && manager.recursos[tipo].available_amount >= cola->top->amount) {
        PendingRequest* pending = queue_dequeue(cola);
        manager.recursos[tipo].available_amount -= pending->amount;
        printf("[DEQUEUE] Job %d desencolado (tipo %d, amount %d, available_restante %d)\n",
               pending->job_id, tipo, pending->amount, manager.recursos[tipo].available_amount);
        
        // Enviar cada job dado a su dueño correspondiente
        snprintf(msg, sizeof(msg), "JOB_GRANTED %d\n", pending->job_id);
        enqueue_write(g_epfd, pending->owner_conn, msg);

        // Liberar el recurso dado
        free(pending);
    }
    pthread_mutex_unlock(&manager.recursos[tipo].mutex);

}

/**
 * ======================================================================================
 * Funciones de Resource Manager
 * ======================================================================================
 */

// Inicializa el gestor de recursos del agente en C local
void manager_init(int cpu, int mem, int gpu) {
    manager.recursos[RESOURCE_CPU].available_amount = manager.recursos[RESOURCE_CPU].total_amount = cpu;
    manager.recursos[RESOURCE_MEM].available_amount = manager.recursos[RESOURCE_MEM].total_amount = mem;
    manager.recursos[RESOURCE_GPU].available_amount = manager.recursos[RESOURCE_GPU].total_amount = gpu;
    
    tabla_jobs_init(&manager.tabla);
    tabla_conns_init(&conns);
    tabla_nodos_init();

    queue_init(&manager.recursos[RESOURCE_CPU].cola);
    queue_init(&manager.recursos[RESOURCE_MEM].cola);
    queue_init(&manager.recursos[RESOURCE_GPU].cola);

    pthread_mutex_init(&manager.recursos[RESOURCE_CPU].mutex, NULL);
    pthread_mutex_init(&manager.recursos[RESOURCE_MEM].mutex, NULL);
    pthread_mutex_init(&manager.recursos[RESOURCE_GPU].mutex, NULL);

    return;
}


// Destruye el gestor de recursos del agente en C local, liberando la memoria
void manager_destroy() {
    pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
    queue_destroy(&manager.recursos[RESOURCE_CPU].cola);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
    queue_destroy(&manager.recursos[RESOURCE_MEM].cola);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
    queue_destroy(&manager.recursos[RESOURCE_GPU].cola);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

    tabla_jobs_destroy(&manager.tabla);
    tabla_conns_destroy(&conns);
    tabla_nodos_destroy();

    pthread_mutex_destroy(&manager.recursos[RESOURCE_CPU].mutex);
    pthread_mutex_destroy(&manager.recursos[RESOURCE_MEM].mutex);
    pthread_mutex_destroy(&manager.recursos[RESOURCE_GPU].mutex);

    return;
}

/**
 * Dentro de handle_tcp_read, luego de cambiar (\n) por (\0).
 * Debe decidir qué hacer con el mensaje, si RESERVE, RELEASE, etc.
 */
void process_message(connection_t *conn, char *msg) {
    // Identificación del origen para el log de mi consola
    const char *origen = (conn->type == CONN_TCP_CLIENT_LOCAL) ? "ERLANG LOCAL" : "AGENTE REMOTO";
    printf("[RX] De %s (fd %d) -> %s\n", origen, conn->fd, msg);
    printf("[DEBUG_BYTES] len=%zu primeros_bytes=", strlen(msg));
    for (size_t i = 0; i < strlen(msg) && i < 15; i++) {
        printf("%02x ", (unsigned char)msg[i]);
    }
    printf("\n");
    fflush(stdout);
    // Se parsea la primera palabra usando caso por caso dependiendo del primer comando
    /**
     * RESERVE <job_id> <recurso> <amount>
     */
    if (strncmp(msg, "RESERVE", 7) == 0) {
        reserve_msg_t result = parse_reserve(msg);
        if (result.valido) {
            result_t r = reserve_resource(result.type, result.amount, result.job_id, conn);
            if (r == RM_DENIED) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "DENIED %d\n", result.job_id);
                enqueue_write(g_epfd, conn, buf);
            }
            else if (r == RM_GRANTED) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "GRANTED %d\n", result.job_id);
                enqueue_write(g_epfd, conn, buf);
            }
        } else {
            fprintf(stderr, "[!] RESERVE mal formado: %s\n", msg);
        }
    }
    /**
     * RELEASE <job_id>
     */
    else if (strncmp(msg, "RELEASE", 7) == 0) {
        release_msg_t result = parse_release(msg);

        if (result.valido) {
            release_resource(result.type, result.amount);
            tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);

            // PURGA DE FANTASMAS
            pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);
        }
        else {
            fprintf(stderr, "RELEASE mal formado %s\n", msg);
        }
    }
    /**
     * GRANTED <job_id>
     */
    else if (strncmp(msg, "GRANTED", 7) == 0) {
        granted_msg_t result = parse_granted(msg);
        if (result.valido) {
            /**
             * Se procesa el pendiente con el job_id dado
             * Si la lista de pendientes a procesar se vacía, avisar con algún indicador y enviar un JOB_GRANTED a conn
             */
            const char *ip_remoto = tabla_conns_get_ip_by_conn(&conns, conn);
            if (ip_remoto == NULL) {
                fprintf(stderr, "GRANTED de una conexión desconocida\n");
            }
            pthread_mutex_lock(&manager.tabla.lock);
            bool confirmado = tabla_jobs_confirmar(&manager.tabla, ip_remoto, result.job_id);

            if (confirmado) {
                unsigned int idx = result.job_id % TAM_TABLA_JOBS;
                Job *j = manager.tabla.tabla_jobs[idx];
                while (j != NULL) {
                    if (j->job_id == result.job_id) break;
                    j = j->sig;
                }

                if (j != NULL && j->pendientes == NULL) {
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", result.job_id);
                    printf("[TX] A ERLANG LOCAL (fd %d) -> %s", j->conn->fd, buf);
                    enqueue_write(g_epfd, j->conn, buf);
                }
            }
            else {

            }
            pthread_mutex_unlock(&manager.tabla.lock);

        } else {
            fprintf(stderr, "[!] GRANTED mal formado: %s\n", msg);
        }
    }
    /**
     * DENIED <job_id>
     */
    else if (strncmp(msg, "DENIED", 6) == 0) {
        denied_msg_t result = parse_denied(msg);
        if (result.valido) {
            connection_t *owner = tabla_jobs_get_conn(&manager.tabla ,result.job_id);
            if (owner != NULL) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", result.job_id);
                printf("[TX] A ERLANG LOCAL (fd %d) -> %s", owner->fd, buf);
                enqueue_write(g_epfd, owner, buf);
            }
        } else {
            fprintf(stderr, "DENIED mal formado: %s\n", msg);
        }
    }
    /**
     * JOB_REQUEST <job_id> [<host>:<recurso>:<amount> ....]
     */
    else if (strncmp(msg, "JOB_REQUEST", 11) == 0) {
        job_request_t result = parse_job_request(msg);

        if (!result.valido) {
            fprintf(stderr, "JOB_REQUEST mal formado %s\n", msg);
            return;
        }

        // Obtener lista de recursos a solicitar
        /**
         * 1ra pasada: Recursos locales
         */
        resource_request_t *curr = result.request_list;
        while (curr != NULL) {
            if (strcmp(curr->ip, g_ip) == 0) {
                result_t r = reserve_resource(curr->type, curr->amount, result.job_id, conn);
                if (r == RM_DENIED) {
                    pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
                    queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
                    pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

                    pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
                    queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
                    pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

                    pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
                    queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
                    pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

                    tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);

                    /**
                     * TODO: Enviar un mensaje de DENIED a quien hizo este request
                     */
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf),  "JOB_DENIED %d\n", result.job_id);
                    printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
                    enqueue_write(g_epfd, conn, buf);
                    return;
                }
                else if (r == RM_GRANTED || r == RM_QUEUED) {
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, g_ip, NULL, NULL, g_ip);
                }
            }

            curr = curr->next;
        }

        /**
         * 2da pasada: Recursos remotos
         */
        curr = result.request_list;
        while (curr != NULL) {
            if (strcmp(curr->ip, g_ip) != 0) {
                connection_t *remote = tabla_conns_lookup(&conns, curr->ip);

                if (remote != NULL) {
                    char msg[BUFF_SIZE];
                    snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n", result.job_id, resource_type_to_str(curr->type), curr->amount);
                    enqueue_write(g_epfd, remote, msg);
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, curr->ip, remote, NULL, g_ip);
                }
                else {
                    /**
                     * Iniciar o conectar con conexión asíncrona
                     */
                    int puerto = tabla_nodos_get_puerto(curr->ip);
                    connection_t *n = NULL;

                    if (puerto != -1) n = connect_remote_node(g_epfd, curr->ip, puerto);

                    if (n == NULL) {
                        pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
                        queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
                        pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

                        pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
                        queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
                        pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

                        pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
                        queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
                        pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

                        tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);

                        char buf[BUFF_SIZE];
                        snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", result.job_id);
                        printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
                        enqueue_write(g_epfd, conn, buf);
                        return;
                    }
                    else {
                        // Enviar mensaje de solicitud de RESERVE adecuado
                        char buf[BUFF_SIZE];
                        snprintf(buf, sizeof(buf), "RESERVE %d %s %d\n", result.job_id, resource_type_to_str(curr->type), curr->amount);
                        tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, curr->ip, n, buf, g_ip);
                    }
                }
            }
            curr = curr->next;
        }

        resource_list_destroy(result.request_list);
        
    }
    /**
     * GET_NODES
     */
    else if (strncmp(msg, "GET_NODES", 9) == 0) {
        char buf[BUFF_SIZE];
        strcpy(buf, "NODES ");
        
        pthread_mutex_lock(&tabla_nodos.mutex);
        for (int i = 0; i < TAM_TABLA_CONN; i++) {
            NodeEntry *curr = tabla_nodos.buckets[i];
            while (curr != NULL) {
                char tmp[128];
                // Formato: 192.168.1.10:8100:cpu:4:mem:8192:gpu:1;
                snprintf(tmp, sizeof(tmp), "%s:%d:cpu:%d:mem:%d:gpu:%d;", 
                         curr->ip, curr->puerto, curr->cpu_disp, curr->mem_disp, curr->gpu_disp);
                
                // Evitar desbordamiento de buffer de forma rudimentaria
                if (strlen(buf) + strlen(tmp) < BUFF_SIZE - 2) {
                    strcat(buf, tmp);
                }
                curr = curr->next;
            }
        }
        pthread_mutex_unlock(&tabla_nodos.mutex);
        
        // Sacar el último ';' si existe y poner \n
        size_t len = strlen(buf);
        if (len > 6 && buf[len-1] == ';') buf[len-1] = '\n';
        else strcat(buf, "\n");

        printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
        enqueue_write(g_epfd, conn, buf);
    }
    /**
     * ANNOUNCE
     */
    else if (strncmp(msg, "ANNOUNCE", 8) == 0) {
        /**
         * Esta parte fue hecha en process_announce, por lo que no es necesaria, pero se mantiene acá por completitud
         */
    }
    /**
     * JOB_RELEASE <job_id>
     */
    else if (strncmp(msg, "JOB_RELEASE", 11) == 0) {
        job_release_msg_t result = parse_job_release(msg);
        if (result.valido) {

            char buf[BUFF_SIZE];
            snprintf(buf, sizeof(buf), "RELEASE %d cpu 0\nRELEASE %d mem 0\nRELEASE %d gpu 0\n", 
                     result.job_id, result.job_id, result.job_id);

            printf("[LOCK] intentando tomar tabla_conns.mutex en BROADCAST (JOB_RELEASE)\n"); fflush(stdout);
            pthread_mutex_lock(&conns.mutex);
            printf("[LOCK] tomado tabla_conns.mutex en BROADCAST\n"); fflush(stdout);
            for (int i = 0; i < TAM_TABLA_CONN; i++) {
                ConnEntry *curr = conns.buckets[i];
                while (curr != NULL) {
                    printf("[TX] BROADCAST INTERNO A AGENTE %s (fd %d) -> %s", curr->ip, curr->conn->fd, buf);
                    enqueue_write(g_epfd, curr->conn, buf);
                    curr = curr->next;
                }
            }
            pthread_mutex_unlock(&conns.mutex);

            tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);

            // PURGA DE FANTASMAS
            pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);
        }
        else {
            fprintf(stderr, "JOB_RELEASE mal formado: %s\n", msg);
        }
    }
    /**
     * JOB_STATUS <job_id>
     */
    else if (strncmp(msg, "JOB_STATUS", 10) == 0) {
        
        job_status_msg_t result = parse_job_status(msg);
        if (result.valido) {
            char buf[BUFF_SIZE];
            if (tabla_jobs_get_conn(&manager.tabla, result.job_id) != NULL) {
                snprintf(buf, sizeof(buf), "JOB_STATUS %d ACTIVE\n", result.job_id);
            } else {
                snprintf(buf, sizeof(buf), "JOB_STATUS %d UNKNOWN\n", result.job_id);
            }
            printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
            enqueue_write(g_epfd, conn, buf);
        }
        else {
            fprintf(stderr, "JOB_STATUS mal formado: %s\n", msg);
        }
    
    }
    else if (strncmp(msg, "JOB_DENIED", 10) == 0) {
        job_denied_msg_t result = parse_job_denied(msg);
        
        if (result.valido) {
            /**
             * TODO: Elimnar todos los Jobs adecuados
             */
            pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

            tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);
        }
    }
    else if (strncmp(msg, "JOB_TIMEOUT", 11) == 0) {
        job_timeout_msg_t result = parse_job_timeout(msg);

        if (result.valido) {
            pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

            pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
            queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, result.job_id);
            pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

            tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd);
        }
    }
    /**
     * Comando inválido
     */
    else {
        fprintf(stderr, "Comando inválido: %s\n", msg);
    }

    return;
}

/**
 * Dentro de handle_udp_read, por cada datagrama que llega al socket UDP.
 * Para agregarlo a los conocidos.
 */
void process_announce(const char *ip_sender, const char *message) {
    announce_msg_t result = parse_announce(message);
    if (result.valido)
        tabla_nodos_insert_or_update(ip_sender, result.puerto, result.cpu, result.mem, result.gpu);
    else
        fprintf(stderr, "ANNOUNCE mal formado: %s\n", message);
    return;
}


/**
 * Llamar antes de destruir el socket
 */
void process_disconnect(connection_t *conn) {
    // Primero, para cada uno de los recursos, es necesario liberarlo de la cola
    pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
    queue_delete_by_conn(&manager.recursos[RESOURCE_CPU].cola, conn);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
    queue_delete_by_conn(&manager.recursos[RESOURCE_MEM].cola, conn);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
    queue_delete_by_conn(&manager.recursos[RESOURCE_GPU].cola, conn);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

    // Luego, para la tabla de jobs, es necesario eliminarlos de la tabla de Jobs
    Allocation *lista = tabla_jobs_delete_by_conn(&manager.tabla, conn);

    /**
     * Liberar cada recurso remoto
     */
    char buf[BUFF_SIZE];
    while (lista != NULL) {
        Allocation* n = lista->sig;

        if (lista->type == REMOTE) {
            connection_t *remote = tabla_conns_lookup(&conns, lista->ip);
            if (remote != NULL) {
                snprintf(buf, sizeof(buf), "RELEASE %d\n", lista->job_id);
                enqueue_write(g_epfd, remote, buf);
            }
        }
        free(lista);
        lista = n;
    }

    // Finalmente, se elimina la conexión de la tabla de nodos conectados
    tabla_conns_delete_by_conn(&conns, conn);
    return;
}

/**
 * Se llama cuando un connect_remote_node finaliza con éxito.
 * Acá el Gestor de Estado ya puede llamar a enqueue_write con sus peticiones.
 */
void process_connection_ready(connection_t *conn) {
    /*
    pthread_mutex_lock(&mutex_pendientes_salientes);

    OutReq *prev = NULL;
    OutReq *curr = pendientes_salientes;

    while (curr != NULL) {
        OutReq *next = curr->next;

        if (curr->conn == conn) {
            if (prev == NULL) {
                pendientes_salientes = next;
            } else {
                prev->next = next;
            }

            tabla_conns_insert(curr->ip, conn);
            enqueue_write(g_epfd, conn, curr->msg);
            free(curr);
            curr = next;
        } else {
            prev = curr;
            curr = next;
        }
    }


    pthread_mutex_unlock(&mutex_pendientes_salientes);
    return;
    */

    /**
     * TODO: En vez de correr por la lista de OutRequests, hay que ir para cada lista de OutRequests en la TablaJobs
     * y enviar el mensaje de RESERVE que se quería enviar
     * NOTA: Podría simplemente retornar una lista gigante de todos los OutRequests con esta conexión y hacer el envío
     */
    OutRequest* lista = tabla_jobs_get_pendientes_by_conn(&manager.tabla, conn);

    while (lista != NULL) {
        enqueue_write(g_epfd, conn, lista->msg);
        lista = lista->next;
    }

    // Eliminar toda esta lista
    while (lista != NULL) {
        OutRequest *next = lista->next;
        free(lista);
        lista = next;
    }
    return;
}

/**
 * Se llama si connect_remote_node falló (ej. el nodo B estaba apagado).
 * El Gestor de Estado debe abortar su plan y quizás buscar otro nodo.
 */
void process_connection_failed(connection_t *conn) {
    /*
    pthread_mutex_lock(&mutex_pendientes_salientes);

    OutReq *prev = NULL;
    OutReq *curr = pendientes_salientes;

    while (curr != NULL) {
        OutReq *next = curr->next;

        if (curr->conn == conn) {
            if (prev == NULL) {
                pendientes_salientes = next;
            } else {
                prev->next = next;
            }
            fprintf(stderr, "Conexion fallida hacia %s, descartando pedido: %s\n", curr->ip, curr->msg);
            
            // Le avisa a erlang que falló
            int job_id;
            if (sscanf(curr->msg, "RESERVE %d", &job_id) == 1) {
                connection_t *owner = buscar_job_owner(job_id);
                if (owner != NULL) {
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", job_id);
                    enqueue_write(g_epfd, owner, buf);
                }
                eliminar_job_owner(job_id);
            }
            
            free(curr);
            curr = next;
        } else {
            prev = curr;
            curr = next;
        }
    }

    pthread_mutex_unlock(&mutex_pendientes_salientes);
    return;
    */

    /**
     * TODO: En vez de recorrer de esta forma, ir por cada bucket en TablaJobs para enviar el JOB_DENIED de
     * cada uno
     * NOTA: Retornar una lista de todos los OutRequests con esta conexión y enviar el mensaje. Fachilito
     */
    pthread_mutex_lock(&manager.tabla.lock);

    Job* lista =  tabla_jobs_extract_by_remote_conn(&manager.tabla, conn);
    while (lista != NULL) {
        Job *sig = lista->sig;

        pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
        queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, lista->job_id);
        pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

        pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
        queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, lista->job_id);
        pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

        pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
        queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, lista->job_id);
        pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

        Allocation *all = lista->confirmadas;
        while (all != NULL) {
            Allocation *n = all->sig;
            if (all->type == LOCAL)
                release_resource(all->name, lista->job_id);
            else if(all->type == REMOTE) {
                connection_t *remote = tabla_conns_lookup(&conns, all->ip);
                if (remote != NULL) {
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf), "RELEASE %d\n", lista->job_id);
                    enqueue_write(g_epfd, remote, buf);
                }
            }
            free(all);
            all = n;
        }

        OutRequest *sal = lista->pendientes;
        while (sal != NULL) {
            OutRequest* n = sal->next;
            free(sal);
            sal = n;
        }

        free(lista);
        lista = sig;
    }

    pthread_mutex_unlock(&manager.tabla.lock);
}
