#include <stdlib.h>
#include <stdbool.h>
#include "tabla_jobs.h"
#include <string.h>

void tabla_jobs_init(TablaJobs *j) {
    memset(j->tabla_jobs, 0, sizeof(j->tabla_jobs));
    pthread_mutex_init(&j->mutex, NULL);
    return;
}

void tabla_jobs_destroy(TablaJobs *j) {
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
bool buscar_job_tabla(Job *j, int job_id) {
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
connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id) {
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

void tabla_jobs_insert(TablaJobs *j, connection_t *conn, int job_id, resource_t type, int amount) {
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