#include <stdlib.h>
#include <stdbool.h>
#include "tabla_jobs.h"
#include <string.h>
#include <stdio.h>

/**
 * ======================================================================================================
 * Funciones auxiliares
 * ======================================================================================================
 */

bool buscar_job_tabla(Job *j, int job_id)
{
    Job *curr = j;
    while (curr != NULL)
    {
        if (curr->job_id == job_id)
            return true;
        curr = curr->sig;
    }
    return false;
}

// ======================================================================================================

void tabla_jobs_init(TablaJobs *j) {
    memset(j->tabla_jobs, 0, sizeof(j->tabla_jobs));
    pthread_mutex_init(&j->lock, NULL);
    return;
}

void tabla_jobs_destroy(TablaJobs *j) {
    pthread_mutex_lock(&j->lock);
    for (int i = 0; i < TAM_TABLA_JOBS; i++) {
        Job* job = j->tabla_jobs[i];

        // Cada bucket es una lista enlazada, por lo que es esencial eliminar cada uno de sus elementos
        while (job != NULL) {
            Job* next =  job->sig;

            // Liberar cada allocation
            Allocation* curr = job->confirmadas;

            while (curr != NULL) {
                Allocation* n = curr->sig;
                free(curr);
                curr = n;
            }

            OutRequest *out = job->pendientes;
            while(out != NULL) {
                OutRequest *n = out->next;
                free(out->ip);
                free(out);
                out = n;
            }

            free(job);
            job = next;
        }

        j->tabla_jobs[i] = NULL;
    }
    pthread_mutex_unlock(&j->lock);
    pthread_mutex_destroy(&j->lock);
    return;
}

bool tabla_jobs_get_id(TablaJobs *t, int job_id) {
    pthread_mutex_lock(&t->lock);
    unsigned int idx = job_id % TAM_TABLA_JOBS;
    
    Job* curr = t->tabla_jobs[idx];

    while (curr != NULL) {
        if (curr->job_id == job_id) {
            pthread_mutex_unlock(&t->lock);
            return true;
        }
        curr = curr->sig;
    }
    pthread_mutex_unlock(&t->lock);
    return false;
}

/**
 * Busca el conn original asociado a un job_id, sin importar el bucket.
 * Devuelve NULL si no se encuentra.
 */
connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id) {
    pthread_mutex_lock(&j->lock);
    unsigned int idx = job_id % TAM_TABLA_JOBS;
    Job *curr = j->tabla_jobs[idx];

    // La búsqueda por conexión es una búsqueda en ua lista simplemente enlazada
    while (curr != NULL) {
        if (curr->job_id == job_id) {
            connection_t *c = curr->conn;
            pthread_mutex_unlock(&j->lock);
            return c;
        }
        curr = curr->sig;
    }
    pthread_mutex_unlock(&j->lock);
    return NULL;
}

void tabla_jobs_insert(TablaJobs *j, connection_t *conn, int job_id, resource_t type, int amount, int max_amount, char* ip, connection_t *remoto, const char *buf) {
    pthread_mutex_lock(&j->lock);
    unsigned int idx = job_id % TAM_TABLA_JOBS;

    // Si el Job no está allí, se crea un Job nuevo con una lista de Allocations vacía
    if (!buscar_job_tabla(j->tabla_jobs[idx], job_id)) {
        Job* job = malloc(sizeof(Job));
        job->conn = conn;
        job->job_id = job_id;
        job->confirmadas = NULL;

        job->sig = j->tabla_jobs[idx];
        j->tabla_jobs[idx] = job;
    }

    // Sino, se busca el Job y se inserta en la lista adecuada
    Job* curr = j->tabla_jobs[idx];
    while (curr != NULL) {
        if (curr->job_id == job_id) {
            // Si viene del agente local, entonces guardar en Allocations
            if (strcmp(g_ip, ip) == 0) {
                // Es una inserción a la cabeza de una lista simplemente enlazada
                Allocation *all = malloc(sizeof(Allocation));
                all->amount = amount;
                all->name = type;
                all->type = LOCAL;

                all->sig = curr->confirmadas;
                curr->confirmadas = all;
                break;
            }
            // Sino, se guarda en OutRequests
            else {
                OutRequest *out = malloc(sizeof(OutRequest));
                out->conn = remoto;
                strncpy(out->ip, ip, sizeof(out->ip)-1);
                out->ip[sizeof(ip) - 1] = '\0';
                out->conn = remoto;
                strncpy(out->msg, buf, sizeof(buf));

                out->next = curr->pendientes;
                curr->pendientes = out;
                break;
            }
        }
        curr = curr->sig;
    }
    pthread_mutex_unlock(&j->lock);
}

/**
 * Remover un Job de la tabla de Jobs, ya sea porque ya se solicitó todos los requests o por una desconexión
 */
void tabla_jobs_remove(TablaJobs *j, int job_id) {
    pthread_mutex_lock(&j->lock);
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
            allocs_to_release = start->confirmadas;
            free(start);
            break;
        }
        prev = start;
        start = start->sig;
    }

    // Iterar y liberar los recursos fuera de la zona crítica de la tabla
    Allocation *all = allocs_to_release;
    while (all != NULL) {
        Allocation *sig = all->sig;
        release_resource(all->name, all->amount); // Nótese que release_resource nunca usa funciones de la tabla
        free(all);
        all = sig;
    }

    pthread_mutex_unlock(&j->lock);
    return;
}

/**
 * Recorre toda la tabla buscando jobs de la conexión dada, libera sus recursos
 * (vía release_resource, que SÍ toma su propio lock) y elimina los jobs.
 * NO se llama con j->lock tomado, porque release_resource necesita tomar
 * el mutex de cada Recurso y no queremos anidar locks innecesariamente.
 */
Allocation* tabla_jobs_delete_by_conn(TablaJobs *j, connection_t *conn) {
    pthread_mutex_lock(&j->lock);

    Allocation *allocs_to_release_head = NULL;
    OutRequest *request_a_liberar_head = NULL;

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

                Allocation *all = curr->confirmadas;
                if (all != NULL) {
                    Allocation *tail = all;
                    while(tail->sig != NULL) tail = tail->sig;
                    tail->sig = allocs_to_release_head;
                    allocs_to_release_head = all;
                }

                OutRequest *rel = curr->pendientes;
                if (rel != NULL) {
                    OutRequest *tail = rel;
                    while (tail->next != NULL) tail = tail->next;
                    free(rel);
                    rel = tail;
                }
                
                free(curr);
                curr = next;
            } else {
                prev = curr;
                curr = next;
            }
        }
    }
    pthread_mutex_unlock(&j->lock);

    return allocs_to_release_head;
}

OutRequest* tabla_jobs_get_pendientes_by_conn(TablaJobs *j, connection_t *conn) {
    /**
     * TODO:
     */
    return NULL;
}

/**
 * Toma la solicitud pendiente con el IP enviado, y lo transfiere a la solicitud confirmada del Job dado
 * CUIDADO: Esta función no es thread-safe. Asume que se ha tomado tabla->lock antes de entrar acá
 */
bool tabla_jobs_confirmar(TablaJobs *j, const char *ip, int job_id) {
    unsigned int idx = job_id % TAM_TABLA_JOBS;

    Job* curr = j->tabla_jobs[idx];
    while (curr != NULL) {
        if (curr->job_id == job_id) break;
        curr = curr->sig;
    }

    if (curr == NULL)
        return false;

    // Buscar el saliente con la dirección IP dada
    OutRequest *sal = curr->pendientes;
    OutRequest *prev = NULL;

    while (sal != NULL) {
        if (strcmp(sal->ip, ip) == 0) {
            if (prev == NULL)
                curr->pendientes = sal->next;
            else
                prev->next = sal->next;
            
            // Crear nuevo Allocation
            Allocation *nuevo = malloc(sizeof(Allocation));
            nuevo->amount = sal->amount;
            nuevo->name = sal->tipo;
            nuevo->type = REMOTE;
            strncpy(nuevo->ip, sal->ip, sizeof(sal->ip) - 1);
            nuevo->ip[sizeof(sal->ip) - 1] = '\0';
            nuevo->job_id = job_id;

            nuevo->sig = curr->confirmadas;
            curr->confirmadas = nuevo;

            free(sal);

            return true;

        }
        prev = sal;
        sal = sal->next;
    }
    return false;
}

Job* tabla_jobs_extract_by_remote_conn(TablaJobs *j, connection_t *conn) {
    pthread_mutex_lock(&j->lock);
    
    Job *extraidos = NULL;  // lista de jobs a retornar
    
    for (int idx = 0; idx < TAM_TABLA_JOBS; idx++) {
        Job *prev = NULL;
        Job *curr = j->tabla_jobs[idx];
        
        while (curr != NULL) {
            Job *next = curr->sig;
            
            // Chequear si algún OutRequest de este job tiene esa conn
            bool afectado = false;
            OutRequest *out = curr->pendientes;
            while (out != NULL) {
                if (out->conn == conn) { afectado = true; break; }
                out = out->next;
            }
            
            if (afectado) {
                // Desenlazar de la tabla
                if (prev == NULL) j->tabla_jobs[idx] = next;
                else prev->sig = next;
                
                // Agregar a la lista de extraídos
                curr->sig = extraidos;
                extraidos = curr;
                curr = next;
            } else {
                prev = curr;
                curr = next;
            }
        }
    }
    
    pthread_mutex_unlock(&j->lock);
    return extraidos;
}
