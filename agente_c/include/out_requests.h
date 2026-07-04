#ifndef OUT_REQUESTS_H
#define OUT_REQUESTS_H

#include <pthread.h>

typedef struct OutReq {
    connection_t *conn;
    char msg[BUFF_SIZE];
    char ip[15];
    struct OutReq *next;
} OutRequest;

typedef struct {
    OutRequest *head;
    int cant_requests;
} ListaOutRequest;

/**
 * Inicializa la lista de solicitudes de nodos remotos
 */
void lista_out_requests_init(ListaOutRequest *l);

/**
 * Destruye la lista de solicitudes de nodos remotos
 */
void lista_out_requests_destroy(ListaOutRequest *l);

#endif