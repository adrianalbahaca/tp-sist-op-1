#ifndef PENDING_REQUEST_H
#define PENDING_REQUEST_H

#include <stdbool.h>
#include "server.h"

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

void queue_init(ColaPendingRequest* c);

void queue_destroy(ColaPendingRequest* c);

bool queue_is_empty(ColaPendingRequest *c);

void queue_delete_by_conn(ColaPendingRequest *c, connection_t *conn);

void queue_delete_by_job_id(ColaPendingRequest *c, int job_id);

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
void queue_enqueue(ColaPendingRequest *c, int job_id, int amount, connection_t* conn);

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
PendingRequest* queue_dequeue(ColaPendingRequest *c);

#endif