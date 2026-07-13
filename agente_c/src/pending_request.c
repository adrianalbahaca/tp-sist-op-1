#include <stdlib.h>
#include "pending_request.h"
#include <stdbool.h>
#include <stdio.h>

char* type_to_str(origin_t origen) {
    switch(origen) {
        case ORIGIN_LOCAL:
            return "LOCAL";
            break;
        case ORIGIN_REMOTE:
            return "REMOTE";
            break;
        default:
            return NULL;
            break;
    }
}

void queue_print(ColaPendingRequest *c) {
    printf("[QUEUE]: [");
    for (PendingRequest *nodo = c->top; nodo != NULL; nodo = nodo->sig) {
        printf("job_id: %d, amount: %d, type: %s ->", nodo->job_id, nodo->amount, type_to_str(nodo->origen));
    }
    printf(" NULL]\n");
    return;
}

void queue_init(ColaPendingRequest* c) {
    c->top = NULL;
    c->bottom = NULL;
    c->amount = 0;
}

void queue_destroy(ColaPendingRequest* c) {
    PendingRequest* curr = c->top;
    while (curr != NULL) {
        PendingRequest* next = curr->sig;
        free(curr);
        curr = next;
    }
    c->top = NULL;
    c->bottom = NULL;
}

bool queue_is_empty(ColaPendingRequest *c) {
    return (c->top == NULL && c->bottom == NULL);
}

void queue_delete_by_conn(ColaPendingRequest *c, connection_t *conn) {
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

            c->amount -= curr->amount;
            free(curr);
            curr = next;
        }
        else {
            prev = curr;
            curr = next;
        }
    }
    queue_print(c);
    return;
}

void queue_delete_by_job_id(ColaPendingRequest *c, int job_id) {
    PendingRequest *curr = c->top;
    PendingRequest *prev = NULL;
    while (curr != NULL) {
        PendingRequest *next = curr->sig;
        if (curr->job_id == job_id) {
            if (prev == NULL) c->top = next;
            else prev->sig = next;

            if (c->bottom == curr) c->bottom = prev;

            c->amount -= curr->amount;
            free(curr);
            curr = next; 
        } else {
            prev = curr;
            curr = next;
        }
    }
    queue_print(c);
    return;
}

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado el mutex del recurso antes
 */
bool queue_enqueue(ColaPendingRequest *c, int job_id, int amount, connection_t* conn, int max_amount, origin_t origen) {
    
    PendingRequest* pending = malloc(sizeof(PendingRequest));
    pending->job_id = job_id;
    pending->amount = amount;
    pending->owner_conn = conn;
    pending->origen = origen;
    pending->sig = NULL;

    if (queue_is_empty(c)) {
        c->top = pending;
        c->bottom = pending;
    }
    else {
        c->bottom->sig = pending;
        c->bottom = pending;
    }
    c->amount += amount;

    queue_print(c);
    return true;
}

/**
 * CUIDADO: Esta función no es thread-safe por sí misma. Asume que se ha tomado el mutex del recurso antes
 */
PendingRequest* queue_dequeue(ColaPendingRequest *c) {
    if (!queue_is_empty(c)) {
        PendingRequest* tope = c->top;
        c->top = tope->sig;

        if (c->top == NULL) {
            c->bottom = NULL;
        }

        tope->sig = NULL;

        c->amount -= tope->amount;
        queue_print(c);
        return tope;
    }

    else {
        queue_print(c);
        return NULL;
    }
}
