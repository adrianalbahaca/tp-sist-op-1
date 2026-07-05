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

typedef struct OutReq {
    connection_t *conn;
    char msg[BUFF_SIZE];
    char ip[16];
    struct OutReq *next;
} OutReq;

static OutReq *pendientes_salientes = NULL;
static pthread_mutex_t mutex_pendientes_salientes = PTHREAD_MUTEX_INITIALIZER;

/**
 * Registro simple job_id -> conn original, para poder reenviar GRANTED/DENIED
 * que llegan de nodos remotos hacia quien hizo el JOB_REQUEST.
 */
typedef struct JobOwner {
    int job_id;
    connection_t *conn;
    resource_request_t *pending_requests;
    struct JobOwner *next;
} JobOwner;

static JobOwner *job_owners = NULL;
static pthread_mutex_t mutex_job_owners = PTHREAD_MUTEX_INITIALIZER;

// Ahora almacena la lista enlazada de peticiones pendientes
static void registrar_job_owner(int job_id, connection_t *conn, resource_request_t *reqs) {
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *o = malloc(sizeof(JobOwner));
    o->job_id = job_id;
    o->conn = conn;
    o->pending_requests = reqs;
    o->next = job_owners;
    job_owners = o;
    pthread_mutex_unlock(&mutex_job_owners);
}
/*
static bool decrementar_job_grants(int job_id) {
    pthread_mutex_lock(&mutex_job_owners);
    bool completo = false;
    JobOwner *curr = job_owners;
    while (curr != NULL) {
        if (curr->job_id == job_id) {
            curr->pending_grants--;
            if (curr->pending_grants == 0) completo = true;
            break;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&mutex_job_owners);
    return completo;
}*/

static char* recurso_type_a_string(resource_t type) {
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
            break;
    }
}

static connection_t* buscar_job_owner(int job_id) {
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *curr = job_owners;
    while (curr != NULL) {
        if (curr->job_id == job_id) {
            connection_t *c = curr->conn;
            pthread_mutex_unlock(&mutex_job_owners);
            return c;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&mutex_job_owners);
    return NULL;
}

static void eliminar_job_owner(int job_id) {
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *curr = job_owners;
    JobOwner *prev = NULL;

    while (curr != NULL) {
        if (curr->job_id == job_id) {
            if (prev == NULL) job_owners = curr->next;
            else prev->next = curr->next;
            
            // Purga de la lista de peticiones en caso de aborto prematuro (DENIED)
            resource_request_t *req = curr->pending_requests;
            while(req != NULL) {
                resource_request_t *next = req->next;
                free(req);
                req = next;
            }
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&mutex_job_owners);
}

/**
 * El gestor de recursos se define estáticamente para trabajar con memoria dinámica lo menos posible y evitar complicaciones
 */
static ResourceManager manager;

/**
 * Se ajusta el epoll local para que lo use el manager
 */
static int g_epfd;

void manager_set_epoll(int epfd) {
    g_epfd = epfd;
}

char g_ip[16];

void manager_set_ip(const char *ip) {
    strncpy(g_ip, ip, sizeof(g_ip) - 1);
    g_ip[sizeof(g_ip) - 1] = '\0';
    return;
}

/**
 * =====================================================================================
 * Funciones Auxiliares de la TablaJobs
 * =====================================================================================
 */

/**
 * =====================================================================================
 * Funciones de Recurso
 * =====================================================================================
 */

static const char* resource_type_to_str(resource_t tipo) {
    if (tipo == RESOURCE_CPU) return "cpu";
    if (tipo == RESOURCE_MEM) return "mem";
    return "gpu";
}

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
static void release_resource(resource_t tipo, int amount) {
    pthread_mutex_lock(&manager.recursos[tipo].mutex);
    manager.recursos[tipo].available_amount += amount;

    ColaPendingRequest *cola = &manager.recursos[tipo].cola;
    
    // Lista temporal para almacenar los trabajos que logran salir de la cola
    PendingRequest *granted_list = NULL;
    PendingRequest *granted_tail = NULL;

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
    tabla_conns_init();
    tabla_nodos_init();

    queue_init(&manager.recursos[RESOURCE_CPU].cola);
    queue_init(&manager.recursos[RESOURCE_MEM].cola);
    queue_init(&manager.recursos[RESOURCE_GPU].cola);

    pthread_mutex_init(&manager.recursos[RESOURCE_CPU].mutex, NULL);
    pthread_mutex_init(&manager.recursos[RESOURCE_MEM].mutex, NULL);
    pthread_mutex_init(&manager.recursos[RESOURCE_GPU].mutex, NULL);

    return;
}

void avanzar_reserva(int job_id) {
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *curr = job_owners;
    while (curr != NULL) {
        if (curr->job_id == job_id) break;
        curr = curr->next;
    }
    
    if (curr == NULL) {
        printf("[AVANZAR_RESERVA] Job %d: NO se encontró en job_owners. Se ignora silenciosamente.\n", job_id);
        fflush(stdout);
        pthread_mutex_unlock(&mutex_job_owners);
        return;
    }

    if (curr->pending_requests == NULL) {
        // Fin de la secuencia. Transacción distribuida exitosa.
        connection_t *conn_erlang = curr->conn;
        pthread_mutex_unlock(&mutex_job_owners);
        
        char buf[BUFF_SIZE];
        snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", job_id);
        printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn_erlang->fd, buf);
        enqueue_write(g_epfd, conn_erlang, buf);
        eliminar_job_owner(job_id);
        return;
    }

    // Extraer atómicamente el próximo recurso de la secuencia
    resource_request_t *req = curr->pending_requests;
    curr->pending_requests = req->next;
    connection_t *conn_erlang = curr->conn;
    pthread_mutex_unlock(&mutex_job_owners);

    if (strcmp(req->ip, g_ip) == 0) {
        // Recurso local
        result_t r = reserve_resource(req->type, req->amount, job_id, conn_erlang);
        if (r == RM_GRANTED) {
            tabla_jobs_insert(&manager.tabla, conn_erlang, job_id, req->type, req->amount);
            free(req);
            avanzar_reserva(job_id); // Continúa la cadena instantáneamente
        } else {
            // RM_QUEUED: Detiene la ejecución. release_resource reanudará la cadena.
            free(req);
        }
    } else {
        // Recurso remoto
        char mensaje[BUFF_SIZE];
        snprintf(mensaje, sizeof(mensaje), "RESERVE %d %s %d\n", job_id, resource_type_to_str(req->type), req->amount);

        connection_t *remote = tabla_conns_lookup(req->ip);
        if (remote != NULL) {
            printf("[TX] A AGENTE REMOTO %s (fd %d) -> %s", req->ip, remote->fd, mensaje);
            enqueue_write(g_epfd, remote, mensaje);
        } else {
            int puerto = tabla_nodos_get_puerto(req->ip);
            connection_t *nueva = NULL;
            if (puerto != -1) nueva = connect_remote_node(g_epfd, req->ip, puerto);

            if (nueva == NULL) {
                fprintf(stderr, "Fallo crítico de enrutamiento hacia %s. Abortando Job %d.\n", req->ip, job_id);
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", job_id);
                enqueue_write(g_epfd, conn_erlang, buf);
                eliminar_job_owner(job_id);
            } else {
                // Esto es una inserción sobre la lista en la cabeza
                // Creación del OutRequests
                OutReq *out = malloc(sizeof(OutReq));
                out->conn = nueva;
                strncpy(out->msg, mensaje, sizeof(out->msg) - 1);
                out->msg[sizeof(out->msg) - 1] = '\0';
                strncpy(out->ip, req->ip, sizeof(out->ip) - 1);
                out->ip[sizeof(out->ip) - 1] = '\0';

                // Inserción en la cabeza de la lista
                pthread_mutex_lock(&mutex_pendientes_salientes);
                out->next = pendientes_salientes;
                pendientes_salientes = out;
                pthread_mutex_unlock(&mutex_pendientes_salientes);
                printf("[I] Iniciando conexión asíncrona hacia %s para enviar: %s", req->ip, mensaje);
            }
        }
        free(req);
    }
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
    tabla_conns_destroy();
    tabla_nodos_destroy();

    pthread_mutex_destroy(&manager.recursos[RESOURCE_CPU].mutex);
    pthread_mutex_destroy(&manager.recursos[RESOURCE_MEM].mutex);
    pthread_mutex_destroy(&manager.recursos[RESOURCE_GPU].mutex);

    pthread_mutex_lock(&mutex_pendientes_salientes);
    OutReq *curr_req = pendientes_salientes;
    while (curr_req != NULL) {
        OutReq *next = curr_req->next;
        free(curr_req);
        curr_req = next;
    }
    pendientes_salientes = NULL;
    pthread_mutex_unlock(&mutex_pendientes_salientes);
    pthread_mutex_destroy(&mutex_pendientes_salientes);

    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *curr_owner = job_owners;
    while (curr_owner != NULL) {
        JobOwner *next = curr_owner->next;
        free(curr_owner);
        curr_owner = next;
    }
    job_owners = NULL;
    pthread_mutex_unlock(&mutex_job_owners);
    pthread_mutex_destroy(&mutex_job_owners);

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
                char* buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "DENIED %d\n", result.job_id);
                enqueue_write(g_epfd, conn, buf);
            }
            else if (r == RM_GRANTED) {
                char* buf[BUFF_SIZE];
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
            tabla_jobs_remove(&manager.tabla, result.job_id);

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
            avanzar_reserva(result.job_id);
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
            connection_t *owner = buscar_job_owner(result.job_id);
            if (owner != NULL) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", result.job_id);
                printf("[TX] A ERLANG LOCAL (fd %d) -> %s", owner->fd, buf);
                enqueue_write(g_epfd, owner, buf);
            }
            eliminar_job_owner(result.job_id);
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

                    tabla_jobs_remove(&manager.tabla, result.job_id);

                    /**
                     * TODO: Enviar un mensaje de DENIED a quien hizo este request
                     */
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf),  "JOB_DENIED %d\n", result.job_id);
                    printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
                    enqueue_write(g_epfd, conn, buf);
                }
                else if (r == RM_GRANTED) {
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, g_ip);
                }
            }

            curr = curr->next;
        }

        /**
         * 2da pasada: Recursos remotos
         */
        curr = result.request_list;
        while (curr != NULL) {
            char buf[BUFF_SIZE];
            if (strcmp(curr->ip, g_ip) != 0) {
                // Insertar en la tabla, sabiendo que están pendientes
                snprintf(buf, sizeof(buf), "RESERVE %d %s %d\n", result.job_id, recurso_type_a_string(curr->type), curr->amount);
                enqueue_write(g_epfd, conn, buf);
                tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, curr->ip);
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
            pthread_mutex_lock(&tabla_conns.mutex);
            printf("[LOCK] tomado tabla_conns.mutex en BROADCAST\n"); fflush(stdout);
            for (int i = 0; i < TAM_TABLA_CONN; i++) {
                ConnEntry *curr = tabla_conns.buckets[i];
                while (curr != NULL) {
                    printf("[TX] BROADCAST INTERNO A AGENTE %s (fd %d) -> %s", curr->ip, curr->conn->fd, buf);
                    enqueue_write(g_epfd, curr->conn, buf);
                    curr = curr->next;
                }
            }
            pthread_mutex_unlock(&tabla_conns.mutex);

            tabla_jobs_remove(&manager.tabla, result.job_id);
            eliminar_job_owner(result.job_id);

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
    tabla_jobs_delete_by_conn(&manager.tabla, conn);

    // Limpiar el rastreador de dueños para evitar Use-After-Free
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *prev_o = NULL;
    JobOwner *curr_o = job_owners;
    while (curr_o != NULL) {
        if (curr_o->conn == conn) {
            JobOwner *to_delete = curr_o;
            
            if (prev_o == NULL) {
                job_owners = curr_o->next;
                curr_o = job_owners; 
            } else {
                prev_o->next = curr_o->next;
                curr_o = curr_o->next;
            }
            free(to_delete);
        } else {
            prev_o = curr_o;
            curr_o = curr_o->next;
        }
    }
    pthread_mutex_unlock(&mutex_job_owners);

    // Limpiar la sala de espera saliente
    pthread_mutex_lock(&mutex_pendientes_salientes);
    OutReq *prev_r = NULL;
    OutReq *curr_r = pendientes_salientes;
    while (curr_r != NULL) {
        if (curr_r->conn == conn) {
            OutReq *next_r = curr_r->next;
            if (prev_r == NULL) pendientes_salientes = next_r;
            else prev_r->next = next_r;
            free(curr_r);
            curr_r = next_r;
        } else {
            prev_r = curr_r;
            curr_r = curr_r->next;
        }
    }
    pthread_mutex_unlock(&mutex_pendientes_salientes);

    // Finalmente, se elimina la conexión de la tabla de nodos conectados
    tabla_conns_delete_by_conn(conn);
    return;
}

/**
 * Se llama cuando un connect_remote_node finaliza con éxito.
 * Acá el Gestor de Estado ya puede llamar a enqueue_write con sus peticiones.
 */
void process_connection_ready(connection_t *conn) {
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
}

/**
 * Se llama si connect_remote_node falló (ej. el nodo B estaba apagado).
 * El Gestor de Estado debe abortar su plan y quizás buscar otro nodo.
 */
void process_connection_failed(connection_t *conn) {
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
}
