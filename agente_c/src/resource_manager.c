#include "../include/resource_manager.h"
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

static ResourceManager manager;

/**
 * ======================================================================================
 * Cola de pendientes
 * ======================================================================================
 */

/**
 * ======================================================================================
 * Tabla de Jobs
 * ======================================================================================
 */

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
}

void manager_destroy() {
    queue_destroy(&manager.pending_cpu);
    queue_destroy(&manager.pending_mem);
    queue_destroy(&manager.pending_gpu);
}