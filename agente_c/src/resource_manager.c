#include "../include/resource_manager.h"
#include "../include/server.h"
#include "../include/protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include "../include/resource_types.h"

#define TAM_TABLA_JOBS 71 // Se selecciona un numero primo chico por cuestiones de optimización

typedef enum {
    RM_GRANTED,
    RM_QUEUED,
    RM_DENIED
} result_t;

// La tabla de jobs será una tabla hash con función de hasheo simple
typedef struct Allocation {
    resource_t name;
    int amount;
    struct Allocation *sig;
} Allocation;

typedef struct Job {
    int job_id;
    connection_t *conn;

    Allocation *allocations;

    struct Job *sig;
} Job;

// La cola de requests pendientes se implementará con una lista simplemente enlazada
typedef struct PendingRequest{
    int job_id;
    int amount;
    connection_t *owner_conn;

    struct PendingRequest *sig; 
} PendingRequest;

typedef struct {
    PendingRequest *top;
    PendingRequest *bottom;
} ColaPendingRequest;

typedef struct {
    int available_amount;
    int total_amount;
    ColaPendingRequest cola;
    pthread_mutex_t mutex;
} Recurso;

typedef struct {
    Job* tabla_jobs[TAM_TABLA_JOBS];
    pthread_mutex_t mutex;
} TablaJobs;

typedef struct {
    Recurso recursos[3];
    TablaJobs tabla;
} ResourceManager;

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
/**
 * ======================================================================================
 * Cola de pendientes
 * ======================================================================================
 */

static void queue_init(ColaPendingRequest* c) {
    c->top = NULL;
    c->bottom = NULL;
}

static void queue_destroy(ColaPendingRequest* c) {
    PendingRequest* curr = c->top;
    while (curr != NULL) {
        PendingRequest* next = curr->sig;
        free(curr);
        curr = next;
    }
    c->top = NULL;
    c->bottom = NULL;
}

static bool queue_is_empty(ColaPendingRequest *c) {
    return (c->top == NULL && c->bottom == NULL);
}

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
static void queue_enqueue(ColaPendingRequest *c, int job_id, int amount, connection_t* conn) {
    PendingRequest* pending = malloc(sizeof(PendingRequest));
    pending->job_id = job_id;
    pending->amount = amount;
    pending->owner_conn = conn;
    pending->sig = NULL;

    if (queue_is_empty(c)) {
        c->top = pending;
        c->bottom = pending;
    }
    else {
        c->bottom->sig = pending;
        c->bottom = pending;
    }
    return;
}

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
static PendingRequest* queue_dequeue(ColaPendingRequest *c) {
    if (!queue_is_empty(c)) {
        PendingRequest* tope = c->top;
        c->top = tope->sig;

        if (c->top == NULL) {
            c->bottom = NULL;
        }
        
        tope->sig = NULL;
        return tope;
    }

    else {
        return NULL;
    }
}

/**
 * ======================================================================================
 * Tabla de Jobs
 * ======================================================================================
 */

static void table_init(TablaJobs *j) {
    memset(j->tabla_jobs, 0, TAM_TABLA_JOBS * sizeof(j->tabla_jobs));
    pthread_mutex_init(&j->mutex, NULL);
    return;
}

static void table_destroy(TablaJobs *j) {
    pthread_mutex_lock(&j->mutex);
    for (int i = 0; i < TAM_TABLA_JOBS; i++) {
        Job* job = j->tabla_jobs[i];

        // Cada bucket es una lista enlazada, por lo que es esencial eliminar cada uno de sus elementos
        while (job != NULL) {
            Job* next =  job->sig;

            // Liberar cada allocation
            Allocation* curr = job->allocations;

            while (curr != NULL) {
                Allocation* n = curr->sig;
                free(curr);
                curr = n;
            }

            free(job);
            job = next;
        }

        j->tabla_jobs[i] = NULL;
    }
    pthread_mutex_unlock(&j->mutex);
    pthread_mutex_destroy(&j->mutex);
    return;
}

static void table_insert(TablaJobs *j) {
    return;
}

static void table_remove(TablaJobs *j) {
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
    else {
        queue_enqueue(&manager.recursos[tipo].cola, job_id, amount, conn);
        result = RM_QUEUED;
    }

    pthread_mutex_unlock(&manager.recursos[tipo].mutex);

    return result;
}

// Atiende una orden RELEASE, liberando la memoria y la cola acorde
static void release_resource (resource_t tipo, int amount) {
    pthread_mutex_lock(&manager.recursos[tipo].mutex);
    manager.recursos[tipo].available_amount += amount;

    ColaPendingRequest *cola = &manager.recursos[tipo].cola;
    while (!queue_is_empty(cola) &&
            manager.recursos[tipo].available_amount >= cola->top->amount) {
        PendingRequest* pending = queue_dequeue(cola);
        manager.recursos[tipo].available_amount -= pending->amount;
        char buf[BUFF_SIZE];
        snprintf(buf, sizeof(buf), "GRANTED %d", pending->job_id);
        enqueue_write(g_epfd, pending->owner_conn, buf);
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
    
    table_init(&manager.tabla);

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

    table_destroy(&manager.tabla);

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
    // Se parsea la primera palabra usando caso por caso dependiendo del primer comando
    if (strncmp(msg, "RESERVE", 7) == 0) {
        reserve_msg_t result = parse_reserve(msg);
        if (result.valido) {
            // Enviar mensaje de que se pudo pedir lo solicitado
            result_t r = reserve_resource(result.type, result.amount, result.job_id, conn);
            if (r == RM_GRANTED) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "GRANTED %d", result.job_id);
                enqueue_write(g_epfd, conn, buf);
            }
        }
        else {
            fprintf(stderr, "RESERVE mal formado %s\n", msg);
        }
    }
    else if (strncmp(msg, "RELEASE", 7) == 0) {
        release_msg_t result = parse_release(msg);

        if (result.valido) {
            release_resource(result.type, result.amount);
        }
        else {
            fprintf(stderr, "RELEASE mal formado %s\n", msg);
        }
    }
    else if (strncmp(msg, "GRANTED", 7) == 0) {
        granted_msg_t result = parse_granted(msg);

        if (result.valido) {
            fprintf(stderr, "GRANTED recibido del job %d\n", result.job_id);
            /**
             * TODO: Una vez se tenga la orquestación multi-nodo, se envía al nodo correspondiente
             */
        }
        else {
            fprintf(stderr, "GRANTED mal formado: %s\n", msg);
        }
    }
    else if (strncmp(msg, "DENIED", 6) == 0) {
        denied_msg_t result = parse_denied(msg);

        if (result.valido) {
            fprintf(stderr, "DENIED recibido del job %d\n", result.job_id);
            /**
             * TODO: Una vez se tenga la orquestación multi-nodo, se envía al nodo correspondiente
             */
        }
        else {
            fprintf(stderr, "DENIED mal formado: %s\n", msg);
        }
    }
    else if (strncmp(msg, "JOB_REQUEST", 11) == 0) {
        /**
         * TODO: Parsear para el caso local, y después extender para el caso multi-nodo
         */
    }
    return;
}

/**
 * Dentro de handle_udp_read, por cada datagrama que llega al socket UDP.
 * Para agregarlo a los conocidos.
 */
void process_announce(const char *ip_sender, const char *message) {
    return;
}


/**
 * Llamar antes de destruir el socket
 */
void process_disconnect(connection_t *conn) {
    return;
}

/**
 * Se llama cuando un connect_remote_node finaliza con éxito.
 * Acá el Gestor de Estado ya puede llamar a enqueue_write con sus peticiones.
 */
void process_connection_ready(connection_t *conn) {
    return;
}

/**
 * Se llama si connect_remote_node falló (ej. el nodo B estaba apagado).
 * El Gestor de Estado debe abortar su plan y quizás buscar otro nodo.
 */
void process_connection_failed(connection_t *conn) {
    return;
}