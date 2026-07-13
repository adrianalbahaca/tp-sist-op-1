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
#include <assert.h>
#include <stdarg.h>


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

typedef enum
{
    DEST_ERLANG_LOCAL,
    DEST_AGENTE_REMOTO
} dest_t;

// Al principio de resource_manager.c, después de los #include
static void send_message(connection_t *conn, int epfd, dest_t dest, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

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

void limpiar_colas(int job_id) {
    pthread_mutex_lock(&manager.recursos[RESOURCE_CPU].mutex);
    queue_delete_by_job_id(&manager.recursos[RESOURCE_CPU].cola, job_id);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_CPU].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_MEM].mutex);
    queue_delete_by_job_id(&manager.recursos[RESOURCE_MEM].cola, job_id);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_MEM].mutex);

    pthread_mutex_lock(&manager.recursos[RESOURCE_GPU].mutex);
    queue_delete_by_job_id(&manager.recursos[RESOURCE_GPU].cola, job_id);
    pthread_mutex_unlock(&manager.recursos[RESOURCE_GPU].mutex);

    return;
}

void send_message(connection_t *conn, int epfd, dest_t dest, const char *fmt, ...) {
    char buf[BUFF_SIZE];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    const char *dest_str = (dest == DEST_ERLANG_LOCAL) ? "ERLANG LOCAL" : "AGENTE REMOTO";
    printf("[TX] A %s (fd %d) -> %s", dest_str, conn->fd, buf);

    enqueue_write(epfd, conn, buf);
}

 /**
 * =====================================================================================
 * Funciones de Recurso
 * =====================================================================================
 */

// Atiende una orden RESERVE y devuelve una respuesta acorde
static result_t reserve_resource (resource_t tipo, int amount, int job_id, connection_t *conn, origin_t origen) {
    pthread_mutex_lock(&manager.recursos[tipo].mutex);
    
    result_t result;

    if (manager.recursos[tipo].available_amount >= amount) {
        manager.recursos[tipo].available_amount -= amount;
        result = RM_GRANTED;
    }
    else if (manager.recursos[tipo].total_amount < amount) {
        result = RM_DENIED;
    }
    else if (queue_enqueue(&manager.recursos[tipo].cola, job_id, amount, conn, manager.recursos[tipo].total_amount, origen)) {
        result = RM_QUEUED;
    }
    else result = RM_DENIED;

    pthread_mutex_unlock(&manager.recursos[tipo].mutex);

    return result;
}

// Atiende una orden RELEASE, liberando la memoria y la cola acorde
void release_resource(resource_t tipo, int amount) {
    pthread_mutex_lock(&manager.tabla.lock);
    pthread_mutex_lock(&manager.recursos[tipo].mutex);
    manager.recursos[tipo].available_amount += amount;

    ColaPendingRequest *cola = &manager.recursos[tipo].cola;

    if (queue_is_empty(cola)) {
        printf("[RELEASE] tipo %d: cola vacía, nada para desencolar (available=%d)\n", tipo, manager.recursos[tipo].available_amount);
    } 
    else if (manager.recursos[tipo].available_amount < cola->top->amount) {
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
        /*
        snprintf(msg, sizeof(msg), "GRANTED %d\n", pending->job_id);
        enqueue_write(g_epfd, pending->owner_conn, msg);
        */
        // Primero, verificar que ya está este 
        tabla_jobs_cambio_alloc(&manager.tabla, pending->job_id, pending->owner_conn, false);

        if (pending->origen == ORIGIN_REMOTE) {
            snprintf(msg, sizeof(msg), "GRANTED %d\n", pending->job_id);
            enqueue_write(g_epfd, pending->owner_conn, msg);
        }
        else if (pending->origen == ORIGIN_LOCAL && tabla_jobs_verificar(&manager.tabla, pending->job_id, false)) {
            snprintf(msg, sizeof(msg), "JOB_GRANTED %d\n", pending->job_id);
            enqueue_write(g_epfd, pending->owner_conn, msg);
        }

        // Liberar el recurso dado
        free(pending);
    }
    pthread_mutex_unlock(&manager.recursos[tipo].mutex);
    pthread_mutex_unlock(&manager.tabla.lock);

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
    // Se parsea la primera palabra usando caso por caso dependiendo del primer comando
    /**
     * RESERVE <job_id> <recurso> <amount>
     */
    if (strncmp(msg, "RESERVE", 7) == 0) {
        reserve_msg_t result = parse_reserve(msg);
        if (result.valido) {
            result_t r = reserve_resource(result.type, result.amount, result.job_id, conn, ORIGIN_REMOTE);
            if (r == RM_DENIED) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "DENIED %d\n", result.job_id);
                printf("[TX] A AGENTE REMOTO (fd %d) -> %s", conn->fd, buf);
                enqueue_write(g_epfd, conn, buf);
            }
            else if (r == RM_GRANTED) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "GRANTED %d\n", result.job_id);
                printf("[TX] A AGENTE REMOTO (fd %d) -> %s", conn->fd, buf);
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
            if (result.amount != 0)
                release_resource(result.type, result.amount);
            
                tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd, true);

            // PURGA DE FANTASMAS
            limpiar_colas(result.job_id);
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
             * TODO: Revisa de quién fue el Job. Verifica si existe, y extrae el pendiente para moverlo al confirmado
             * Si los confirmados están vacíos y todo salió bien, envía JOB_GRANTED
             */

            char *ip_remoto = tabla_conns_get_ip_by_conn(&conns, conn);

            if (ip_remoto == NULL) {
                fprintf(stderr, "GRANTED de una conexión desconocida\n");
                return;
            }

            pthread_mutex_lock(&manager.tabla.lock);
            Job *check = tabla_jobs_buscar_por_id(&manager.tabla, result.job_id);

            if (check == NULL) {
                // El Job fue liberado anteriormente y tiene que solicitarse que suelte los recursos
                // Para ello, asumiendo que los otros nodos tienen limpieza de fantasmas, se le solicita que liberen vacíos para inducir una limpieza de fantasmas
                send_message(conn, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", result.job_id, "cpu", 0);
                send_message(conn, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", result.job_id, "mem", 0);
                send_message(conn, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", result.job_id, "gpu", 0);
                pthread_mutex_unlock(&manager.tabla.lock);
                return;
            }

            bool confirmado = job_confirmar(check, ip_remoto, result.job_id);

            if (confirmado) {
                bool verificado = tabla_jobs_verificar(&manager.tabla, result.job_id, false);

                if (verificado) {
                    send_message(check->conn, g_epfd, DEST_ERLANG_LOCAL, "JOB_GRANTED %d\n", result.job_id);
                }
            }

            pthread_mutex_unlock(&manager.tabla.lock);
            /*
            const char *ip_remoto = tabla_conns_get_ip_by_conn(&conns, conn);
            if (ip_remoto == NULL) {
                fprintf(stderr, "GRANTED de una conexión desconocida\n");
                return;
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
                
                bool encolados = false;

                for (Allocation *a = j->confirmadas; a != NULL; a = a->sig) {
                    if (a->type == LOCAL && a->result == RM_QUEUED) {
                        encolados = true;
                    }
                }

                if (j->pendientes == NULL && !encolados) {
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", result.job_id);
                    printf("[TX] A ERLANG LOCAL (fd %d) -> %s", j->conn->fd, buf);
                    enqueue_write(g_epfd, j->conn, buf);
                }
            }
            else {

            }
            pthread_mutex_unlock(&manager.tabla.lock);
            */

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

            /**
             * TODO: Revisa quién fue que pidió este Job. Si fue alguien local, libera todo lo relacionado con ese Job
             * y termina
             */
            pthread_mutex_lock(&manager.tabla.lock);
            connection_t *owner = tabla_jobs_get_conn(&manager.tabla, result.job_id, false);
            if (owner != NULL) {
                tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd, false);
                send_message(owner, g_epfd, DEST_ERLANG_LOCAL, "JOB_DENIED %d\n", result.job_id);
            }

            limpiar_colas(result.job_id);
            pthread_mutex_unlock(&manager.tabla.lock);
            /*
            connection_t *owner = tabla_jobs_get_conn(&manager.tabla ,result.job_id);
            if (owner != NULL) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", result.job_id);
                printf("[TX] A ERLANG LOCAL (fd %d) -> %s", owner->fd, buf);
                enqueue_write(g_epfd, owner, buf);
            }
            */
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

        // Obtener listas de requests a intentar
        resource_request_t *head = result.request_list;

        pthread_mutex_lock(&manager.tabla.lock);

        if (tabla_jobs_buscar_por_id(&manager.tabla, result.job_id)) {
            pthread_mutex_unlock(&manager.tabla.lock);
            return;
        }
        /**
         * TODO: Redefinir el JOB_REQUEST, tal que se fabrique el Job en el sitio y no se inserte hasta que
         * esté todo listo
         */

         Job* nuevo_job = malloc(sizeof(Job));
         nuevo_job->job_id = result.job_id;
         nuevo_job->conn = conn;
         nuevo_job->confirmadas = NULL;
         nuevo_job->pendientes = NULL;
        
        /**
         * Ir procesando la lista de requests uno por uno
         */
        while (head != NULL) {
            if (strcmp(head->ip, g_ip) == 0) {
                result_t r = reserve_resource(head->type, head->amount, result.job_id, conn, ORIGIN_LOCAL);
                if (r == RM_GRANTED || r == RM_QUEUED) {
                    // Insertar este nuevo Allocation en el Job
                    Allocation *nuevo = crear_allocation(head->type, head->amount, LOCAL, g_ip, result.job_id, r, conn);

                    if (nuevo_job->confirmadas != NULL)
                        nuevo->sig = nuevo_job->confirmadas;
                    else 
                        nuevo->sig = NULL;

                    nuevo_job->confirmadas = nuevo;

                }
                else if (r == RM_DENIED) {

                    limpiar_colas(result.job_id);

                    // Destruir el Job
                    Allocation *allocs = nuevo_job->confirmadas;
                    OutRequest *outreq = nuevo_job->pendientes;

                    while (allocs != NULL) {
                        if (allocs->type == LOCAL && allocs->result == RM_GRANTED)
                            release_resource(allocs->name, allocs->amount);

                        else if (allocs->type == REMOTE) {
                            connection_t *remote = tabla_conns_lookup(&conns, allocs->ip);
                            if (remote != NULL) {
                                // Funcion para mandar mensaje?
                                send_message(remote, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", allocs->job_id, resource_type_to_str(allocs->name), allocs->amount);
                            }
                        }
                        Allocation *next = allocs->sig;
                        allocs = next;
                    }

                    while (outreq != NULL) {
                        OutRequest *next = outreq->next;
                        free(outreq);
                        outreq = next;
                    }

                    free(nuevo_job);
                    pthread_mutex_unlock(&manager.tabla.lock);
                    send_message(conn, g_epfd, DEST_ERLANG_LOCAL, "JOB_DENIED %d\n", result.job_id);
                    return;
                }

            }
            else {
                connection_t *remote = tabla_conns_lookup(&conns, head->ip);
                if (remote != NULL) {
                    /**
                     * TODO: Enviar mensaje usando función auxiliar
                     */
                    send_message(remote, g_epfd, DEST_AGENTE_REMOTO, "RESERVE %d %s %d\n", result.job_id, resource_type_to_str(head->type), head->amount);
                    
                    // Insertar este nuevo alloc en el Job
                    OutRequest *nuevo = crear_outrequest(head->ip, remote, head->type, head->amount, NULL);

                    if (nuevo_job->pendientes != NULL)
                        nuevo->next = nuevo_job->pendientes;
                    else
                        nuevo->next = NULL;

                    nuevo_job->pendientes = nuevo;
                }

                else {
                    // Tratar de conectarse a una conexión externa ya hecha
                    int puerto = tabla_nodos_get_puerto(head->ip);
                    connection_t *intento = NULL;

                    if (puerto != -1)
                        intento = connect_remote_node(g_epfd, head->ip, puerto);
                    
                    if (intento == NULL) {
                        // Parar todo
                        limpiar_colas(result.job_id);

                        // Borrar Job
                        // Destruir el Job
                        Allocation *allocs = nuevo_job->confirmadas;
                        OutRequest *outreq = nuevo_job->pendientes;

                        while (allocs != NULL) {
                            if (allocs->type == LOCAL && allocs->result == RM_GRANTED)
                                release_resource(allocs->name, allocs->amount);

                            else if (allocs->type == REMOTE) {
                                connection_t *remote = tabla_conns_lookup(&conns, allocs->ip);
                                if (remote != NULL) {
                                    // Funcion para mandar mensaje?
                                    send_message(remote, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", allocs->job_id, resource_type_to_str(allocs->name), allocs->amount);
                                }
                            }
                            Allocation *next = allocs->sig;
                            allocs = next;
                        }

                        while (outreq != NULL) {
                            OutRequest *next = outreq->next;
                            free(outreq);
                            outreq = next;
                        }

                        free(nuevo_job);
                        pthread_mutex_unlock(&manager.tabla.lock);
                        send_message(conn, g_epfd, DEST_ERLANG_LOCAL, "JOB_DENIED %d\n", result.job_id);
                        return;

                        // Enviar mensaje a Erlang de que no se pudo
                    }

                    // Sino, se reserva esta conexión y el mensaje a enviar cuando se confirme que se haya hecho la conexión
                    char respuesta[BUFF_SIZE];
                    snprintf(respuesta, sizeof(respuesta), "RESERVE %d %s %d\n", result.job_id, resource_type_to_str(head->type), head->amount);
                    OutRequest *nuevo = crear_outrequest(head->ip, intento, head->type, head->amount, respuesta);
                    
                    if (nuevo_job->pendientes != NULL)
                        nuevo->next = nuevo_job->pendientes;
                    else
                        nuevo->next = NULL;

                    nuevo_job->pendientes = nuevo;
                }
            }
            head = head->next;
        }

        resource_list_destroy(result.request_list);

        /**
         * TODO: Inserta este nuevo Job recién armado a la tabla
         */
        tabla_jobs_insertar_job(&manager.tabla, nuevo_job);

        pthread_mutex_unlock(&manager.tabla.lock);
        /*
        
        while (curr != NULL) {
            if (strcmp(curr->ip, g_ip) == 0) {
                result_t r = reserve_resource(curr->type, curr->amount, result.job_id, conn, ORIGIN_LOCAL);
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

                    tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd, false);
                    pthread_mutex_unlock(&manager.tabla.lock);

                    // Enviar mensaje JOB_DENIED al mismo Erlang
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf),  "JOB_DENIED %d\n", result.job_id);
                    printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
                    enqueue_write(g_epfd, conn, buf);
                    return;
                }
                else if (r == RM_GRANTED || r == RM_QUEUED) {
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, g_ip, NULL, NULL, g_ip, r, false);
                }
            }

            curr = curr->next;
        }

        
        curr = result.request_list;
        while (curr != NULL) {
            if (strcmp(curr->ip, g_ip) != 0) {
                connection_t *remote = tabla_conns_lookup(&conns, curr->ip);

                if (remote != NULL) {
                    char msg[BUFF_SIZE];
                    snprintf(msg, sizeof(msg), "RESERVE %d %s %d\n", result.job_id, resource_type_to_str(curr->type), curr->amount);
                    printf("[TX] A AGENTE REMOTO (fd %d) -> %s", remote->fd, msg);
                    enqueue_write(g_epfd, remote, msg);
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, curr->ip, remote, NULL, g_ip, -1, false);
                }
                else {
                    
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

                        tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd, false);
                        pthread_mutex_unlock(&manager.tabla.lock);

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
                        tabla_jobs_insert(&manager.tabla, conn, result.job_id, curr->type, curr->amount, manager.recursos[curr->type].total_amount, curr->ip, n, buf, g_ip, -1, false);
                    }
                }
            }
            curr = curr->next;
        }

        resource_list_destroy(result.request_list);
        
        if (tabla_jobs_verificar(&manager.tabla, result.job_id, false)) {
            char buf[BUFF_SIZE];
            snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", result.job_id);
            printf("[TX] A ERLANG LOCAL (fd %d) -> %s", conn->fd, buf);
            enqueue_write(g_epfd, conn, buf);
        }

        pthread_mutex_unlock(&manager.tabla.lock);
        */
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

            pthread_mutex_lock(&manager.tabla.lock);
            Job* eliminar_job = tabla_jobs_extract_by_id(&manager.tabla, result.job_id);

            if (eliminar_job == NULL) {
                pthread_mutex_unlock(&manager.tabla.lock);
                return;
            }

            Allocation* alloc = eliminar_job->confirmadas;
            OutRequest* req = eliminar_job->pendientes;

            while (alloc != NULL) {
                if (alloc->type == LOCAL && alloc->result == RM_GRANTED)
                    release_resource(alloc->name, alloc->amount);
                else
                    limpiar_colas(alloc->job_id);
                
                if (alloc->type == REMOTE) {
                    send_message(alloc->conn, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", alloc->job_id, resource_type_to_str(alloc->name), alloc->amount);
                }

                Allocation *next = alloc->sig;
                free(alloc);
                alloc = next;
            }

            while (req != NULL) {
                OutRequest *next = req->next;
                free(req);
                next = req;   
            }

            free(eliminar_job);

            limpiar_colas(result.job_id);

            pthread_mutex_unlock(&manager.tabla.lock);
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
            if (tabla_jobs_get_conn(&manager.tabla, result.job_id, true) != NULL) {
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
    else if (strncmp(msg, "JOB_TIMEOUT", 11) == 0) {
        job_timeout_msg_t result = parse_job_timeout(msg);

        if (result.valido) {
            tabla_jobs_remove(&manager.tabla, result.job_id, &conns, g_epfd, true);

            limpiar_colas(result.job_id);
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
    while (lista != NULL) {
        Allocation* n = lista->sig;
        if (lista->type == LOCAL && lista->result == RM_GRANTED)
            release_resource(lista->name, lista->amount);

        else if (lista->type == REMOTE) {
            connection_t *remote = tabla_conns_lookup(&conns, lista->ip);
            if (remote != NULL)
                send_message(remote, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", lista->job_id, resource_type_to_str(lista->name), lista->amount);
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
        tabla_conns_insert(&conns, lista->ip, conn);
        printf("[TX] A AGENTE REMOTO (fd %d) -> %s", conn->fd, lista->msg);
        enqueue_write(g_epfd, conn, lista->msg);
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
    Job* lista =  tabla_jobs_extract_by_remote_conn(&manager.tabla, conn);
    while (lista != NULL) {
        Job *sig = lista->sig;

        limpiar_colas(lista->job_id);

        Allocation *all = lista->confirmadas;
        while (all != NULL) {
            Allocation *n = all->sig;
            if (all->type == LOCAL && all->result == RM_GRANTED)
                release_resource(all->name, all->amount);

            else if(all->type == REMOTE) {
                connection_t *remote = tabla_conns_lookup(&conns, all->ip);
                if (remote != NULL) {
                    send_message(remote, g_epfd, DEST_AGENTE_REMOTO, "RELEASE %d %s %d\n", lista->job_id, resource_type_to_str(all->name), all->amount);
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
}
