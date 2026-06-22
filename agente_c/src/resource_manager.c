#include "../include/resource_manager.h"
#include "../include/server.h"
#include "../include/protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>
#include "../include/resource_types.h"

#define TAM_TABLA_JOBS 71 // Se selecciona un numero primo chico por cuestiones de optimización
#define TAM_TABLA_CONN 71

typedef enum {
    RM_GRANTED,
    RM_QUEUED,
    RM_DENIED
} result_t;

static void release_resource(resource_t tipo, int amount);

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

typedef struct ConnEntry{
    char ip[16];
    connection_t *conn;
    struct ConnEntry *next;
} ConnEntry;

typedef struct {
    ConnEntry *buckets[TAM_TABLA_CONN];
    pthread_mutex_t mutex;
} TablaConns;

typedef struct NodeEntry {
    char ip[16];
    int puerto;
    int cpu_disp, mem_disp, gpu_disp;
    time_t last_seen;
    struct NodeEntry *next;
} NodeEntry;

typedef struct {
    NodeEntry *buckets[TAM_TABLA_CONN];
    pthread_mutex_t mutex;
} TablaNodos;

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
    struct JobOwner *next;
} JobOwner;

static JobOwner *job_owners = NULL;
static pthread_mutex_t mutex_job_owners = PTHREAD_MUTEX_INITIALIZER;

static void registrar_job_owner(int job_id, connection_t *conn) {
    pthread_mutex_lock(&mutex_job_owners);
    JobOwner *o = malloc(sizeof(JobOwner));
    o->job_id = job_id;
    o->conn = conn;
    o->next = job_owners;
    job_owners = o;
    pthread_mutex_unlock(&mutex_job_owners);
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
            if (prev == NULL) {
                job_owners = curr->next;
            } else {
                prev->next = curr->next;
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

static TablaConns tabla_conns;
static TablaNodos tabla_nodos;

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

static void queue_delete_by_conn(ColaPendingRequest *c, connection_t *conn) {
    PendingRequest *curr = c->top;
    PendingRequest *prev = NULL;
    while (curr != NULL) {
        PendingRequest *next = curr->sig;
        if (curr->owner_conn == conn) {
            // Si es el tope de la cola
            if (prev == NULL) {
                c->top = curr->sig;
            }
            else {
                prev->sig = next;
            }

            // Si es al final de la cola
            if (c->bottom == curr) {
                c->bottom = prev;
            }

            free(curr);
            curr = next;
        }
        else {
            prev = curr;
            curr = next;
        }
    }
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

static void tabla_jobs_init(TablaJobs *j) {
    memset(j->tabla_jobs, 0, sizeof(j->tabla_jobs));
    pthread_mutex_init(&j->mutex, NULL);
    return;
}

static void tabla_jobs_destroy(TablaJobs *j) {
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

/**
 * ATENCION: Esta función no es thread-safe de por sí. Asume que TablaJobs->mutex esté tomado
 */
static bool buscar_job_tabla(Job *j, int job_id) {
    Job *curr = j;
    while (curr != NULL) {
        if (curr->job_id == job_id)
            return true;
        curr = curr->sig;
    }
    return false;
}

/**
 * Busca el conn original asociado a un job_id, sin importar el bucket.
 * Devuelve NULL si no se encuentra.
 */
static connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id) {
    pthread_mutex_lock(&j->mutex);
    unsigned int idx = job_id % TAM_TABLA_JOBS;
    Job *curr = j->tabla_jobs[idx];
    while (curr != NULL) {
        if (curr->job_id == job_id) {
            connection_t *c = curr->conn;
            pthread_mutex_unlock(&j->mutex);
            return c;
        }
        curr = curr->sig;
    }
    pthread_mutex_unlock(&j->mutex);
    return NULL;
}

static void tabla_jobs_insert(TablaJobs *j, connection_t *conn, int job_id, resource_t type, int amount) {
    pthread_mutex_lock(&j->mutex);
    unsigned int idx = job_id % TAM_TABLA_JOBS;

    if (!buscar_job_tabla(j->tabla_jobs[idx], job_id)) {
        // Crear nuevo Job e insertar el elemento allí
        Job* job = malloc(sizeof(Job));
        job->conn = conn;
        job->job_id = job_id;
        job->allocations = NULL;

        job->sig = j->tabla_jobs[idx];
        j->tabla_jobs[idx] = job;
    }

    // Crear nuevo Allocation a insertar en el Job buscado
    Job* curr = j->tabla_jobs[idx];

    while (curr != NULL) {
        if (curr->job_id == job_id) {
            Allocation *all = malloc(sizeof(Allocation));
            all->amount = amount;
            all->name = type;

            all->sig = curr->allocations;
            curr->allocations = all;
            break;
        }
        curr = curr->sig;
    }

    pthread_mutex_unlock(&j->mutex);
    return;
}

static void tabla_jobs_remove(TablaJobs *j, int job_id) {
    pthread_mutex_lock(&j->mutex);
    unsigned int idx = job_id % TAM_TABLA_JOBS;

    // Recorrer y eliminar de la lista
    Job *prev = NULL;
    Job *start = j->tabla_jobs[idx];

    while (start != NULL) {
        if (start->job_id == job_id) {
            // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
            if (prev == NULL) {
                j->tabla_jobs[idx] = start->sig;
            }
            else {
                prev->sig = start->sig;
            }
            
            // Eliminar elementos de la lista de Allocations
            Allocation * all = start->allocations;
            Allocation *sig;
            while (all != NULL) {
                sig = all->sig;
                release_resource(all->name, all->amount);
                free(all);
                all = sig;
            }
            free(start);
            break;
        }
        prev = start;
        start = start->sig;
    }
    pthread_mutex_unlock(&j->mutex);
    return;
}

/**
 * Recorre toda la tabla buscando jobs de la conexión dada, libera sus recursos
 * (vía release_resource, que SÍ toma su propio lock) y elimina los jobs.
 * NO se llama con j->mutex tomado, porque release_resource necesita tomar
 * el mutex de cada Recurso y no queremos anidar locks innecesariamente.
 */
static void tabla_jobs_delete_by_conn(TablaJobs *j, connection_t *conn) {
    pthread_mutex_lock(&j->mutex);
    for (int idx = 0; idx < TAM_TABLA_JOBS; idx++) {
        Job *prev = NULL;
        Job *curr = j->tabla_jobs[idx];

        while (curr != NULL) {
            Job *next = curr->sig;

            if (curr->conn == conn) {
                if (prev == NULL) {
                    j->tabla_jobs[idx] = next;
                } else {
                    prev->sig = next;
                }

                Allocation *all = curr->allocations;
                while (all != NULL) {
                    Allocation *sig = all->sig;
                    release_resource(all->name, all->amount);
                    free(all);
                    all = sig;
                }
                free(curr);
                curr = next;
            } else {
                prev = curr;
                curr = next;
            }
        }
    }
    pthread_mutex_unlock(&j->mutex);
    return;
}

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
        if (pending->owner_conn->type == CONN_TCP_CLIENT_LOCAL) {
            snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", pending->job_id);
        } else {
            snprintf(buf, sizeof(buf), "GRANTED %d\n", pending->job_id);
        }
        enqueue_write(g_epfd, pending->owner_conn, buf);
        free(pending);
    }

    pthread_mutex_unlock(&manager.recursos[tipo].mutex);
}

/**
 * ======================================================================================
 * Funciones de la tabla de conexiones
 * ======================================================================================
 */
void tabla_conns_init() {
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        tabla_conns.buckets[i] = NULL;
    }
    pthread_mutex_init(&tabla_conns.mutex, NULL);
    return;
}

static unsigned int hash_ip(const char *ip) {
    unsigned int hash = 0;
    while (*ip) {
        hash = hash * 31 + (unsigned char)(*ip);
        ip++;
    }
    return hash % TAM_TABLA_CONN;
}

void tabla_conns_insert(char ip[], connection_t *conn) {
    pthread_mutex_lock(&tabla_conns.mutex);
    unsigned int idx = hash_ip(ip);

    ConnEntry *c = malloc(sizeof(ConnEntry));
    c->conn = conn;
    strncpy(c->ip, ip, 16);

    // Insertar elemento en el bucket dado. Es una inserción en una lista simplemente enlazada
    ConnEntry *start = tabla_conns.buckets[idx];
    c->next = start;

    tabla_conns.buckets[idx] = c;

    pthread_mutex_unlock(&tabla_conns.mutex);
}

connection_t* tabla_conns_lookup(char ip[]) {
    pthread_mutex_lock(&tabla_conns.mutex);
    unsigned int idx = hash_ip(ip);

    // Buscar en la lista enlazada del bucket
    ConnEntry *start = tabla_conns.buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            pthread_mutex_unlock(&tabla_conns.mutex);
            return start->conn;
        }
        start = start->next;
    }
    pthread_mutex_unlock(&tabla_conns.mutex);

    return NULL;
}

void tabla_conns_delete(char ip[]) {
    pthread_mutex_lock(&tabla_conns.mutex);
    unsigned int idx = hash_ip(ip);

    // Eliminar en la lista del bucket
    ConnEntry *prev = NULL;
    ConnEntry *start = tabla_conns.buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
            if (prev == NULL) {
                tabla_conns.buckets[idx] = start->next;
            }
            else {
                prev->next = start->next;
            }
            free(start);
            break;
        }
        prev = start;
        start = start->next;
    }

    pthread_mutex_unlock(&tabla_conns.mutex);
}

void tabla_conns_delete_by_conn(connection_t *conn) {
    pthread_mutex_lock(&tabla_conns.mutex);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        // Eliminar en la lista del bucket
        ConnEntry *prev = NULL;
        ConnEntry *start = tabla_conns.buckets[i];

        while (start != NULL) {
            if (start->conn == conn) {
                // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
                if (prev == NULL) {
                    tabla_conns.buckets[i] = start->next;
                }
                else {
                    prev->next = start->next;
                }
                free(start);
                break;
            }
            prev = start;
            start = start->next;
        }
    }

    pthread_mutex_unlock(&tabla_conns.mutex);
}

void tabla_conns_destroy() {
    pthread_mutex_lock(&tabla_conns.mutex);
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        if (tabla_conns.buckets[i] != NULL) {
            // Destruir la lista de conexiones dadas adentro
            ConnEntry *next;
            while (tabla_conns.buckets[i] != NULL) {
                next = tabla_conns.buckets[i]->next;
                free(tabla_conns.buckets[i]);
                tabla_conns.buckets[i] = next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_conns.mutex);
    pthread_mutex_destroy(&tabla_conns.mutex);
    return;
}

/**
 * ======================================================================================
 * Funciones de la tabla de nodos conocidos (vía UDP ANNOUNCE)
 * ======================================================================================
 */
void tabla_nodos_init() {
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        tabla_nodos.buckets[i] = NULL;
    }
    pthread_mutex_init(&tabla_nodos.mutex, NULL);
    return;
}

/**
 * Si la IP ya existe en la tabla, actualiza sus datos (recursos, timestamp).
 * Si no existe, crea una entrada nueva.
 */
void tabla_nodos_insert_or_update(const char *ip, int puerto, int cpu, int mem, int gpu) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    unsigned int idx = hash_ip(ip);

    NodeEntry *curr = tabla_nodos.buckets[idx];
    while (curr != NULL) {
        if (strcmp(curr->ip, ip) == 0) {
            curr->puerto = puerto;
            curr->cpu_disp = cpu;
            curr->mem_disp = mem;
            curr->gpu_disp = gpu;
            curr->last_seen = time(NULL);
            pthread_mutex_unlock(&tabla_nodos.mutex);
            return;
        }
        curr = curr->next;
    }

    // No existía, se crea una entrada nueva
    NodeEntry *nuevo = malloc(sizeof(NodeEntry));
    strncpy(nuevo->ip, ip, sizeof(nuevo->ip) - 1);
    nuevo->ip[sizeof(nuevo->ip) - 1] = '\0';
    nuevo->puerto = puerto;
    nuevo->cpu_disp = cpu;
    nuevo->mem_disp = mem;
    nuevo->gpu_disp = gpu;
    nuevo->last_seen = time(NULL);
    nuevo->next = tabla_nodos.buckets[idx];
    tabla_nodos.buckets[idx] = nuevo;

    pthread_mutex_unlock(&tabla_nodos.mutex);
    return;
}

/**
 * Busca el puerto asociado a una IP conocida. Devuelve -1 si no se encontró.
 */
int tabla_nodos_get_puerto(const char *ip) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    unsigned int idx = hash_ip(ip);

    NodeEntry *curr = tabla_nodos.buckets[idx];
    while (curr != NULL) {
        if (strcmp(curr->ip, ip) == 0) {
            int puerto = curr->puerto;
            pthread_mutex_unlock(&tabla_nodos.mutex);
            return puerto;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
    return -1;
}

/**
 * Barre la tabla de nodos eliminando aquellos cuyo último anuncio (last_seen)
 * supere el tiempo de expiración (timeout_secs). Requerido por el protocolo de 15 segundos.
 */
void tabla_nodos_purge(int timeout_secs) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    time_t now = time(NULL);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        NodeEntry *prev = NULL;
        NodeEntry *curr = tabla_nodos.buckets[i];

        while (curr != NULL) {
            if (now - curr->last_seen > timeout_secs) {
                NodeEntry *to_delete = curr;
                
                // Desenlazar el nodo
                if (prev == NULL) {
                    tabla_nodos.buckets[i] = curr->next;
                } else {
                    prev->next = curr->next;
                }
                
                curr = curr->next;
                free(to_delete);
            } else {
                prev = curr;
                curr = curr->next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
}


void tabla_nodos_destroy() {
    pthread_mutex_lock(&tabla_nodos.mutex);
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        NodeEntry *next;
        while (tabla_nodos.buckets[i] != NULL) {
            next = tabla_nodos.buckets[i]->next;
            free(tabla_nodos.buckets[i]);
            tabla_nodos.buckets[i] = next;
        }
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
    pthread_mutex_destroy(&tabla_nodos.mutex);
    return;
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
    // Se parsea la primera palabra usando caso por caso dependiendo del primer comando
    if (strncmp(msg, "RESERVE", 7) == 0) {
        reserve_msg_t result = parse_reserve(msg);
        if (result.valido) {
            // Enviar mensaje de que se pudo pedir lo solicitado
            result_t r = reserve_resource(result.type, result.amount, result.job_id, conn);
            if (r == RM_GRANTED) {
                tabla_jobs_insert(&manager.tabla, conn, result.job_id, result.type, result.amount);
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "GRANTED %d\n", result.job_id);
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
            tabla_jobs_remove(&manager.tabla, result.job_id);
        }
        else {
            fprintf(stderr, "RELEASE mal formado %s\n", msg);
        }
    }
    else if (strncmp(msg, "GRANTED", 7) == 0) {
        granted_msg_t result = parse_granted(msg);

        if (result.valido) {
            fprintf(stderr, "GRANTED recibido del job %d\n", result.job_id);
            connection_t *owner = buscar_job_owner(result.job_id);
            if (owner != NULL) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", result.job_id);
                enqueue_write(g_epfd, owner, buf);
            }
        }
        else {
            fprintf(stderr, "GRANTED mal formado: %s\n", msg);
        }
    }
    else if (strncmp(msg, "DENIED", 6) == 0) {
        denied_msg_t result = parse_denied(msg);

        if (result.valido) {
            // fprintf(stderr, "DENIED recibido del job %d\n", result.job_id);
            connection_t *owner = buscar_job_owner(result.job_id);
            if (owner != NULL) {
                char buf[BUFF_SIZE];
                snprintf(buf, sizeof(buf), "JOB_DENIED %d\n", result.job_id);
                enqueue_write(g_epfd, owner, buf);
            }
        }
        else {
            fprintf(stderr, "DENIED mal formado: %s\n", msg);
        }
    }
    else if (strncmp(msg, "JOB_REQUEST", 11) == 0) {
        job_request_t result = parse_job_request(msg);

        if (!result.valido) {
            fprintf(stderr, "JOB_REQUEST mal formado %s\n", msg);
            return;
        }

        resource_request_t *list = result.request_list;

        while (list != NULL) {
            if (strcmp(list->ip, g_ip) == 0) {
                result_t r = reserve_resource(list->type, list->amount, result.job_id, conn);
                if (r == RM_GRANTED) {
                    tabla_jobs_insert(&manager.tabla, conn, result.job_id, list->type, list->amount);
                    char buf[BUFF_SIZE];
                    snprintf(buf, sizeof(buf), "JOB_GRANTED %d\n", result.job_id);
                    enqueue_write(g_epfd, conn, buf);
                }
            }
            else {
                char mensaje[BUFF_SIZE];
                snprintf(mensaje, sizeof(mensaje), "RESERVE %d %s %d\n",
                         result.job_id, resource_type_to_str(list->type), list->amount);

                connection_t *remote = tabla_conns_lookup(list->ip);
                if (remote != NULL) {
                    // Ya hay conexión activa con ese nodo, mandar directo
                    registrar_job_owner(result.job_id, conn);
                    enqueue_write(g_epfd, remote, mensaje);
                } else {
                    // No hay conexión activa: buscar el puerto y conectar
                    int puerto = tabla_nodos_get_puerto(list->ip);
                    if (puerto == -1) {
                        fprintf(stderr, "JOB_REQUEST: nodo %s desconocido, no se puede contactar\n", list->ip);
                    } else {
                        connection_t *nueva = connect_remote_node(g_epfd, list->ip, puerto);
                        if (nueva != NULL) {
                            registrar_job_owner(result.job_id, conn);
                            // Encolar el mensaje pendiente hasta que la conexión esté lista
                            OutReq *req = malloc(sizeof(OutReq));
                            req->conn = nueva;
                            strncpy(req->msg, mensaje, sizeof(req->msg) - 1);
                            req->msg[sizeof(req->msg) - 1] = '\0';
                            strncpy(req->ip, list->ip, sizeof(req->ip) - 1);
                            req->ip[sizeof(req->ip) - 1] = '\0';

                            pthread_mutex_lock(&mutex_pendientes_salientes);
                            req->next = pendientes_salientes;
                            pendientes_salientes = req;
                            pthread_mutex_unlock(&mutex_pendientes_salientes);
                        }
                    }
                }
            }
            list = list->next;
        }
        resource_list_destroy(result.request_list);
    }
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

        enqueue_write(g_epfd, conn, buf);
    }
    else if (strncmp(msg, "ANNOUNCE", 8) == 0) {
        // En process anounce
    }
    else if (strncmp(msg, "JOB_RELEASE", 11) == 0) {
        int job_id;
        if (sscanf(msg, "JOB_RELEASE %d", &job_id) == 1) {
            
            tabla_jobs_remove(&manager.tabla, job_id);

            char buf[BUFF_SIZE];
            snprintf(buf, sizeof(buf), "RELEASE %d cpu 0\nRELEASE %d mem 0\nRELEASE %d gpu 0\n", 
                     job_id, job_id, job_id);

            pthread_mutex_lock(&tabla_conns.mutex);
            for (int i = 0; i < TAM_TABLA_CONN; i++) {
                ConnEntry *curr = tabla_conns.buckets[i];
                while (curr != NULL) {
                    enqueue_write(g_epfd, curr->conn, buf);
                    curr = curr->next;
                }
            }
            pthread_mutex_unlock(&tabla_conns.mutex);

            eliminar_job_owner(job_id);
        }
    }
    else if (strncmp(msg, "JOB_STATUS", 10) == 0) {
        int job_id;
        if (sscanf(msg, "JOB_STATUS %d", &job_id) == 1) {
            char buf[BUFF_SIZE];
            if (tabla_jobs_get_conn(&manager.tabla, job_id) != NULL) {
                snprintf(buf, sizeof(buf), "JOB_STATUS %d ACTIVE\n", job_id);
            } else {
                snprintf(buf, sizeof(buf), "JOB_STATUS %d UNKNOWN\n", job_id);
            }
            enqueue_write(g_epfd, conn, buf);
        }
    }
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
    int puerto, cpu, mem, gpu;
    int n = sscanf(message, "ANNOUNCE %d cpu:%d mem:%d gpu:%d", &puerto, &cpu, &mem, &gpu);
    if (n != 4) {
        fprintf(stderr, "ANNOUNCE mal formado: %s\n", message);
        return;
    }
    tabla_nodos_insert_or_update(ip_sender, puerto, cpu, mem, gpu);
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
            JobOwner *next_o = curr_o->next;
            if (prev_o == NULL) job_owners = next_o;
            else prev_o->next = next_o;
            free(curr_o);
            curr_o = next_o;
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