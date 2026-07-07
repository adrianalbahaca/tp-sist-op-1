#ifndef TABLA_CONNS_H
#define TABLA_CONNS_H

#include "server.h"
#include <pthread.h>

// Se selecciona un numero primo chico por cuestiones de optimización
#define TAM_TABLA_CONN 71

/**
 * La tabla de conexiones externas se crea como una tabla hash donde cada bucket es una lista simplemente
 * enlazada
 */
typedef struct ConnEntry{
    char ip[16];
    connection_t *conn;
    struct ConnEntry *next;
} ConnEntry;

typedef struct {
    ConnEntry *buckets[TAM_TABLA_CONN];
    pthread_mutex_t mutex;
} TablaConns;

/**
 * Inicialización de la tabla de conexiones externas
 */
void tabla_conns_init(TablaConns *t);

/**
 * Se inserta la conexión externa con su IP en caso de tener éxito
 */
void tabla_conns_insert(TablaConns *t, char ip[], connection_t *conn);

/**
 * Se devuelve la conexión externa si fue exitosa. Retorna NULL si no se encuentra
 */
connection_t* tabla_conns_lookup(TablaConns *t, const char ip[]);

/**
 * Se elimina la conexión externa en caso de falla
 */
void tabla_conns_delete(TablaConns *t, char ip[]);

/**
 * Se elimina todas las conexiones que tengan el mismo conn
 */
void tabla_conns_delete_by_conn(TablaConns *t, connection_t *conn);

/**
 * Se retorna la dirección IP de una conexión externa ya completada
 */
const char* tabla_conns_get_ip_by_conn(TablaConns *t, connection_t *conn);

/**
 * Destrucción de la tabla de conexiones externas
 */
void tabla_conns_destroy(TablaConns *t);

#endif
