#include "../include/resource_manager.h"
#include <stdlib.h>
#include <string.h>

#define TAM_TABLA_JOBS 71 // Se selecciona un numero primo chico por cuestiones de optimización

typedef enum {
    RM_GRANTED,
    RM_QUEUED,
    RM_DENIED
} result_t;

typedef struct {
    int available_amount;
    int total_amount;
    char name[16];
} Recurso;

// La tabla de jobs será una tabla hash con función de hasheo simple
typedef struct Allocation {
    char name[16];
    int amount;
    struct Allocation *sig;
} Allocation;

typedef struct {
    int job_id;
    int owner_id;

    Allocation *allocations;
} Job;

// La cola de requests pendientes se implementará con una lista simplemente enlazada
typedef struct PendingRequest{
    int job_id;
    int amount;
    int owner_fd;

    struct PendingRequest *sig; 
} PendingRequest;

typedef struct {
    PendingRequest *top;
    PendingRequest *bottom;
} ColaPendingRequest;

typedef struct {
    Recurso cpu;
    Recurso mem;
    Recurso gpu;

    Job* tabla_jobs[TAM_TABLA_JOBS];

    ColaPendingRequest pending_cpu;
    ColaPendingRequest pending_mem;
    ColaPendingRequest pending_gpu;
} ResourceManager;

/**
 * El gestor de recursos se define estáticamente para trabajar con memoria dinámica lo menos posible y evitar complicaciones
 */
static ResourceManager manager;

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

/**
 * ======================================================================================
 * Tabla de Jobs
 * ======================================================================================
 */

static void table_init(Job** j) {
    memset(j, 0, TAM_TABLA_JOBS * sizeof(*j));
    return;
}

static void table_destroy(Job **j) {
    for (int i = 0; i < TAM_TABLA_JOBS; i++) {
        Job* job = j[i];

        // Cada bucket es una lista enlazada, por lo que es esencial eliminar cada uno de sus elementos
        while (job != NULL) {
            Job* next =  job->allocations;
            free(job);
            job = next;
        }

        j[i] = NULL;
    }
}

/**
 * ======================================================================================
 * Funciones de Resource Manager
 * ======================================================================================
 */

void manager_init(int cpu, int mem, int gpu) {
    manager.cpu.total_amount = manager.cpu.available_amount = cpu;
    manager.mem.total_amount = manager.mem.available_amount = mem;
    manager.gpu.total_amount = manager.gpu.available_amount = gpu;

    table_init(&manager.tabla_jobs);

    queue_init(&manager.pending_cpu);
    queue_init(&manager.pending_mem);
    queue_init(&manager.pending_gpu);

    table_init(&manager.tabla_jobs);

    return;
}

void manager_destroy() {
    queue_destroy(&manager.pending_cpu);
    queue_destroy(&manager.pending_mem);
    queue_destroy(&manager.pending_gpu);

    table_destroy(&manager.tabla_jobs);
    
    return;
}