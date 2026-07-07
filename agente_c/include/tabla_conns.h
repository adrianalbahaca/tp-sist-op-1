#ifndef TABLA_CONNS_H
#define TABLA_CONNS_H

#include "server.h"
#include <pthread.h>

// Se selecciona un numero primo chico por cuestiones de optimización
#define TAM_TABLA_CONN 71

typedef struct ConnEntry{
    char ip[16];
    connection_t *conn;
    struct ConnEntry *next;
} ConnEntry;

typedef struct {
    ConnEntry *buckets[TAM_TABLA_CONN];
    pthread_mutex_t mutex;
} TablaConns;

void tabla_conns_init(TablaConns *t);

void tabla_conns_insert(TablaConns *t, char ip[], connection_t *conn);

connection_t* tabla_conns_lookup(TablaConns *t, const char ip[]);

void tabla_conns_delete(TablaConns *t, char ip[]);

void tabla_conns_delete_by_conn(TablaConns *t, connection_t *conn);

const char* tabla_conns_get_ip_by_conn(TablaConns *t, connection_t *conn);

void tabla_conns_destroy(TablaConns *t);

#endif
