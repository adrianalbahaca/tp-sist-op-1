#include "tabla_conns.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Inicializa la tabla de conexiones
void tabla_conns_init(TablaConns *tabla_conns) {
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        tabla_conns->buckets[i] = NULL;
    }
    pthread_mutex_init(&tabla_conns->mutex, NULL);
    return;
}

// Función hash según IP
static unsigned int hash_ip(const char *ip) {
    unsigned int hash = 0;
    while (*ip) {
        hash = hash * 31 + (unsigned char)(*ip);
        ip++;
    }
    return hash % TAM_TABLA_CONN;
}

// Inserta la conexión en la tabla
void tabla_conns_insert(TablaConns *tabla_conns, char ip[], connection_t *conn) {
    
    pthread_mutex_lock(&tabla_conns->mutex);
    unsigned int idx = hash_ip(ip);

    ConnEntry *c = malloc(sizeof(ConnEntry));
    c->conn = conn;
    strncpy(c->ip, ip, sizeof(c->ip) - 1);
    c->ip[sizeof(c->ip) - 1] = '\0';

    // Insertar elemento en el bucket dado. Es una inserción en una lista simplemente enlazada
    ConnEntry *start = tabla_conns->buckets[idx];
    c->next = start;

    tabla_conns->buckets[idx] = c;

    pthread_mutex_unlock(&tabla_conns->mutex);
}

// Busca la conexión en la tabla y la retorna
connection_t* tabla_conns_lookup(TablaConns *tabla_conns, const char ip[]) {
    pthread_mutex_lock(&tabla_conns->mutex);
    unsigned int idx = hash_ip(ip);

    // Buscar en la lista enlazada del bucket
    ConnEntry *start = tabla_conns->buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            pthread_mutex_unlock(&tabla_conns->mutex);
            return start->conn;
        }
        start = start->next;
    }
    pthread_mutex_unlock(&tabla_conns->mutex);

    return NULL;
}

// Busca la conexión en la tabla y retorna su IP
char* tabla_conns_get_ip_by_conn(TablaConns *tabla_conns, connection_t *conn) {
    pthread_mutex_lock(&tabla_conns->mutex);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        ConnEntry *c = tabla_conns->buckets[i];
        while (c != NULL) {
            if (c->conn == conn) {
                pthread_mutex_unlock(&tabla_conns->mutex);
                return c->ip;
            }
            c = c->next;
        }
    }

    pthread_mutex_unlock(&tabla_conns->mutex);
    return NULL;
}

// Elimina la conexión de la tabla asociada a la IP dada
void tabla_conns_delete(TablaConns *tabla_conns, char ip[]) {
    pthread_mutex_lock(&tabla_conns->mutex);
    unsigned int idx = hash_ip(ip);

    // Eliminar en la lista del bucket
    ConnEntry *prev = NULL;
    ConnEntry *start = tabla_conns->buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
            if (prev == NULL) {
                tabla_conns->buckets[idx] = start->next;
            }
            else {
                prev->next = start->next;
            }
            free(start);
            break;
        }
        prev = start;
        start = start->next;
    }

    pthread_mutex_unlock(&tabla_conns->mutex);
}

// La función anterior pero dada una estructura conexión
void tabla_conns_delete_by_conn(TablaConns *tabla_conns, connection_t *conn) {
    pthread_mutex_lock(&tabla_conns->mutex);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        // Eliminar en la lista del bucket
        ConnEntry *prev = NULL;
        ConnEntry *start = tabla_conns->buckets[i];

        while (start != NULL) {
            if (start->conn == conn) {
                // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
                if (prev == NULL) {
                    tabla_conns->buckets[i] = start->next;
                }
                else {
                    prev->next = start->next;
                }
                free(start);
                break;
            }
            prev = start;
            start = start->next;
        }
    }

    pthread_mutex_unlock(&tabla_conns->mutex);
}

// Destruye la tabla de conexiones
void tabla_conns_destroy(TablaConns *tabla_conns) {
    pthread_mutex_lock(&tabla_conns->mutex);
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        if (tabla_conns->buckets[i] != NULL) {
            // Destruir la lista de conexiones dadas adentro
            ConnEntry *next;
            while (tabla_conns->buckets[i] != NULL) {
                next = tabla_conns->buckets[i]->next;
                free(tabla_conns->buckets[i]);
                tabla_conns->buckets[i] = next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_conns->mutex);
    pthread_mutex_destroy(&tabla_conns->mutex);
    return;
}
