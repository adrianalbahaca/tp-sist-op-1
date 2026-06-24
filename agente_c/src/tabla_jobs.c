#include <stdlib.h>
#include <stdbool.h>
#include "../include/tabla_jobs.h"

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

void tabla_jobs_remove(TablaJobs *j, int job_id) {
    pthread_mutex_lock(&j->mutex);
    unsigned int idx = job_id % TAM_TABLA_JOBS;

    Job *prev = NULL;
    Job *start = j->tabla_jobs[idx];

    Allocation *allocs_to_release = NULL;

    while (start != NULL) {
        if (start->job_id == job_id) {
            // Desenlazar el Job de la tabla
            if (prev == NULL) {
                j->tabla_jobs[idx] = start->sig;
            } else {
                prev->sig = start->sig;
            }
            
            // Extraer la lista de asignaciones antes de liberar el Job
            allocs_to_release = start->allocations;
            free(start);
            break;
        }
        prev = start;
        start = start->sig;
    }
    // IMPORTANTE: Soltar el lock de la tabla ANTES de invocar a release_resource
    pthread_mutex_unlock(&j->mutex);

    // Iterar y liberar los recursos fuera de la zona crítica de la tabla
    Allocation *all = allocs_to_release;
    while (all != NULL) {
        Allocation *sig = all->sig;
        // Ahora release_resource (que invoca tabla_jobs_insert) puede tomar el lock de la tabla sin problemas
        release_resource(all->name, all->amount);
        free(all);
        all = sig;
    }
    return;
}

/**
 * Recorre toda la tabla buscando jobs de la conexión dada, libera sus recursos
 * (vía release_resource, que SÍ toma su propio lock) y elimina los jobs.
 * NO se llama con j->mutex tomado, porque release_resource necesita tomar
 * el mutex de cada Recurso y no queremos anidar locks innecesariamente.
 */
void tabla_jobs_delete_by_conn(TablaJobs *j, connection_t *conn) {
    pthread_mutex_lock(&j->mutex);

    Allocation *allocs_to_release_head = NULL;

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
                if (all != NULL) {
                    Allocation *tail = all;
                    while(tail->sig != NULL) tail = tail->sig;
                    tail->sig = allocs_to_release_head;
                    allocs_to_release_head = all;
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

    Allocation *all = allocs_to_release_head;
    while (all != NULL) {
        Allocation *sig = all->sig;
        release_resource(all->name, all->amount);
        free(all);
        all = sig;
    }
    return;
}