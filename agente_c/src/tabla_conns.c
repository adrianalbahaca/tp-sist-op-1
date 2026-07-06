#include "tabla_conns.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

void tabla_conns_init() {
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        tabla_conns.buckets[i] = NULL;
    }
    pthread_mutex_init(&tabla_conns.mutex, NULL);
    return;
}

static unsigned int hash_ip(const char *ip) {
    unsigned int hash = 0;
    while (*ip) {
        hash = hash * 31 + (unsigned char)(*ip);
        ip++;
    }
    return hash % TAM_TABLA_CONN;
}

void tabla_conns_insert(char ip[], connection_t *conn) {
    printf("[LOCK] intentando tomar tabla_conns.mutex en INSERT (ip=%s)\n", ip); fflush(stdout);
    pthread_mutex_lock(&tabla_conns.mutex);
    printf("[LOCK] tomado tabla_conns.mutex en INSERT\n"); fflush(stdout);
    unsigned int idx = hash_ip(ip);

    ConnEntry *c = malloc(sizeof(ConnEntry));
    c->conn = conn;
    strncpy(c->ip, ip, 16);

    // Insertar elemento en el bucket dado. Es una inserción en una lista simplemente enlazada
    ConnEntry *start = tabla_conns.buckets[idx];
    c->next = start;

    tabla_conns.buckets[idx] = c;

    pthread_mutex_unlock(&tabla_conns.mutex);
}

connection_t* tabla_conns_lookup(char ip[]) {
    printf("[LOCK] intentando tomar tabla_conns.mutex en LOOKUP (ip=%s)\n", ip); fflush(stdout);
    pthread_mutex_lock(&tabla_conns.mutex);
    printf("[LOCK] tomado tabla_conns.mutex en LOOKUP\n"); fflush(stdout);
    unsigned int idx = hash_ip(ip);

    // Buscar en la lista enlazada del bucket
    ConnEntry *start = tabla_conns.buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            pthread_mutex_unlock(&tabla_conns.mutex);
            return start->conn;
        }
        start = start->next;
    }
    pthread_mutex_unlock(&tabla_conns.mutex);

    return NULL;
}

const char* tabla_conns_get_ip_by_conn(connection_t *conn) {
    pthread_mutex_lock(&tabla_conns.mutex);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        ConnEntry *c = tabla_conns.buckets[i];
        while (c != NULL) {
            if (c->conn == conn) {
                pthread_mutex_unlock(&tabla_conns.mutex);
                return c->ip;
            }
            c = c->next;
        }
    }

    pthread_mutex_unlock(&tabla_conns.mutex);
    return NULL;
}

void tabla_conns_delete(char ip[]) {
    printf("[LOCK] intentando tomar tabla_conns.mutex en DELETE (ip=%s)\n", ip); fflush(stdout);
    pthread_mutex_lock(&tabla_conns.mutex);
    printf("[LOCK] tomado tabla_conns.mutex en DELETE\n"); fflush(stdout);
    unsigned int idx = hash_ip(ip);

    // Eliminar en la lista del bucket
    ConnEntry *prev = NULL;
    ConnEntry *start = tabla_conns.buckets[idx];

    while (start != NULL) {
        if (strcmp(start->ip, ip) == 0) {
            // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
            if (prev == NULL) {
                tabla_conns.buckets[idx] = start->next;
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

    pthread_mutex_unlock(&tabla_conns.mutex);
}

void tabla_conns_delete_by_conn(connection_t *conn) {
    printf("[LOCK] intentando tomar tabla_conns.mutex en DELETE_BY_CONN (fd=%d)\n", conn->fd); fflush(stdout);
    pthread_mutex_lock(&tabla_conns.mutex);
    printf("[LOCK] tomado tabla_conns.mutex en DELETE_BY_CONN\n"); fflush(stdout);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        // Eliminar en la lista del bucket
        ConnEntry *prev = NULL;
        ConnEntry *start = tabla_conns.buckets[i];

        while (start != NULL) {
            if (start->conn == conn) {
                // Si es al principio de la lista, es cuestión de actualizar punteros y eliminar
                if (prev == NULL) {
                    tabla_conns.buckets[i] = start->next;
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

    pthread_mutex_unlock(&tabla_conns.mutex);
}

void tabla_conns_destroy() {
    printf("[LOCK] intentando tomar tabla_conns.mutex en DESTROY\n"); fflush(stdout);
    pthread_mutex_lock(&tabla_conns.mutex);
    printf("[LOCK] tomado tabla_conns.mutex en DESTROY\n"); fflush(stdout);
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        if (tabla_conns.buckets[i] != NULL) {
            // Destruir la lista de conexiones dadas adentro
            ConnEntry *next;
            while (tabla_conns.buckets[i] != NULL) {
                next = tabla_conns.buckets[i]->next;
                free(tabla_conns.buckets[i]);
                tabla_conns.buckets[i] = next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_conns.mutex);
    pthread_mutex_destroy(&tabla_conns.mutex);
    return;
}
