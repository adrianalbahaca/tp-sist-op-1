#ifndef PENDING_REQUEST_H
#define PENDING_REQUEST_H

#include <stdbool.h>
#include "server.h"

/**
 * La Cola de Requests Pendientes será una lista simplemente enlazada con punteros a su tope y fondo
 */
typedef struct PendingRequest{
    int job_id;
    int amount;
    connection_t *owner_conn;

    struct PendingRequest *sig; 
} PendingRequest;

typedef struct {
    PendingRequest *top;
    PendingRequest *bottom;
    int amount;
} ColaPendingRequest;

/**
 * Inicializa la cola vacía
 */
void queue_init(ColaPendingRequest* c);

/**
 * Destruye la cola y todo elemento que estaba contenido en ella
 */
void queue_destroy(ColaPendingRequest* c);

/**
 * Verifica si la cola está vacía
 */
bool queue_is_empty(ColaPendingRequest *c);

/**
 * Busca dentro de la cola para eliminar un elemento por la conexión
 */
void queue_delete_by_conn(ColaPendingRequest *c, connection_t *conn);

/**
 * Busca dentro de la cola para eliminar un elemento por el ID de Job
 */
void queue_delete_by_job_id(ColaPendingRequest *c, int job_id);

/**
 * Encola un elemento si es que no supera la cantidad máxima de recurso enviado, aumentando su contador de recurso
 * a solicitar
 * ADVERTENCIA: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
bool queue_enqueue(ColaPendingRequest *c, int job_id, int amount, connection_t* conn, int max_amount);

/**
 * Desencola un elemento y reduce su contador de recurso a utilizar
 * ADVERTENCIA: Esta función no es thread-safe por sí misma. Asume que se ha tomado recurso->m antes
 */
PendingRequest* queue_dequeue(ColaPendingRequest *c);

#endif
