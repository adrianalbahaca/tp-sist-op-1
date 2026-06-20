#include "../include/resource_manager.h"
#include "../include/server.h"
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
 * ======================================================================================
 * Funciones de Resource Manager
 * ======================================================================================
 */



ResourceManager manager_init(int cpu, int mem, int gpu) {
    ResourceManager* manager = malloc(sizeof(ResourceManager));
    manager->recursos[RESOURCE_CPU].available_amount = manager->recursos[RESOURCE_CPU].total_amount = cpu;
    manager->recursos[RESOURCE_MEM].available_amount = manager->recursos[RESOURCE_MEM].total_amount = mem;
    manager->recursos[RESOURCE_GPU].available_amount = manager->recursos[RESOURCE_GPU].total_amount = gpu;
    
    table_init(&manager->tabla);

    queue_init(&manager->recursos[RESOURCE_CPU].cola);
    queue_init(&manager->recursos[RESOURCE_MEM].cola);
    queue_init(&manager->recursos[RESOURCE_GPU].cola);

    pthread_mutex_init(&manager->recursos[RESOURCE_CPU].mutex, NULL);
    pthread_mutex_init(&manager->recursos[RESOURCE_MEM].mutex, NULL);
    pthread_mutex_init(&manager->recursos[RESOURCE_GPU].mutex, NULL);

    return manager;
}

void manager_destroy(ResourceManager* manager) {
    pthread_mutex_lock(&manager->recursos[RESOURCE_CPU].mutex);
    queue_destroy(&manager->recursos[RESOURCE_CPU].cola);
    pthread_mutex_unlock(&manager->recursos[RESOURCE_CPU].mutex);

    pthread_mutex_lock(&manager->recursos[RESOURCE_MEM].mutex);
    queue_destroy(&manager->recursos[RESOURCE_MEM].cola);
    pthread_mutex_unlock(&manager->recursos[RESOURCE_MEM].mutex);

    pthread_mutex_lock(&manager->recursos[RESOURCE_GPU].mutex);
    queue_destroy(&manager->recursos[RESOURCE_GPU].cola);
    pthread_mutex_unlock(&manager->recursos[RESOURCE_GPU].mutex);

    table_destroy(&manager->tabla);

    pthread_mutex_destroy(&manager->recursos[RESOURCE_CPU].mutex);
    pthread_mutex_destroy(&manager->recursos[RESOURCE_MEM].mutex);
    pthread_mutex_destroy(&manager->recursos[RESOURCE_GPU].mutex);

    return;
}