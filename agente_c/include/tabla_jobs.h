#ifndef TABLA_JOBS_H
#define TABLA_JOBS_H

#define TAM_TABLA_JOBS 71 

#include "resource_types.h"
#include "server.h"

#include <pthread.h>

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

typedef struct {
    Job* tabla_jobs[TAM_TABLA_JOBS];
    pthread_mutex_t mutex;
} TablaJobs;

void tabla_jobs_init(TablaJobs *j);

void tabla_jobs_destroy(TablaJobs *j);

/**
 * ATENCION: Esta función no es thread-safe de por sí. Asume que TablaJobs->mutex esté tomado
 */
bool buscar_job_tabla(Job *j, int job_id);

/**
 * Busca el conn original asociado a un job_id, sin importar el bucket.
 * Devuelve NULL si no se encuentra.
 */
connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id);

void tabla_jobs_insert(TablaJobs *j, connection_t *conn, int job_id, resource_t type, int amount);

#endif