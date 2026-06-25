#include <stdlib.h>
#include <string.h>
#include "tabla_node_entry.h"


TablaNodos tabla_nodos;

static unsigned int hash_ip(const char *ip) {
    unsigned int hash = 0;
    while (*ip) {
        hash = hash * 31 + (unsigned char)(*ip);
        ip++;
    }
    return hash % TAM_TABLA_CONN;
}

void tabla_nodos_init() {
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        tabla_nodos.buckets[i] = NULL;
    }
    pthread_mutex_init(&tabla_nodos.mutex, NULL);
    return;
}

/**
 * Si la IP ya existe en la tabla, actualiza sus datos (recursos, timestamp).
 * Si no existe, crea una entrada nueva.
 */
void tabla_nodos_insert_or_update(const char *ip, int puerto, int cpu, int mem, int gpu) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    unsigned int idx = hash_ip(ip);

    NodeEntry *curr = tabla_nodos.buckets[idx];
    while (curr != NULL) {
        if (strcmp(curr->ip, ip) == 0) {
            curr->puerto = puerto;
            curr->cpu_disp = cpu;
            curr->mem_disp = mem;
            curr->gpu_disp = gpu;
            curr->last_seen = time(NULL);
            pthread_mutex_unlock(&tabla_nodos.mutex);
            return;
        }
        curr = curr->next;
    }

    // No existía, se crea una entrada nueva
    NodeEntry *nuevo = malloc(sizeof(NodeEntry));
    strncpy(nuevo->ip, ip, sizeof(nuevo->ip) - 1);
    nuevo->ip[sizeof(nuevo->ip) - 1] = '\0';
    nuevo->puerto = puerto;
    nuevo->cpu_disp = cpu;
    nuevo->mem_disp = mem;
    nuevo->gpu_disp = gpu;
    nuevo->last_seen = time(NULL);
    nuevo->next = tabla_nodos.buckets[idx];
    tabla_nodos.buckets[idx] = nuevo;

    pthread_mutex_unlock(&tabla_nodos.mutex);
    return;
}

/**
 * Busca el puerto asociado a una IP conocida. Devuelve -1 si no se encontró.
 */
int tabla_nodos_get_puerto(const char *ip) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    unsigned int idx = hash_ip(ip);

    NodeEntry *curr = tabla_nodos.buckets[idx];
    while (curr != NULL) {
        if (strcmp(curr->ip, ip) == 0) {
            int puerto = curr->puerto;
            pthread_mutex_unlock(&tabla_nodos.mutex);
            return puerto;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
    return -1;
}

/**
 * Barre la tabla de nodos eliminando aquellos cuyo último anuncio (last_seen)
 * supere el tiempo de expiración (timeout_secs). Requerido por el protocolo de 15 segundos.
 */
void tabla_nodos_purge(int timeout_secs) {
    pthread_mutex_lock(&tabla_nodos.mutex);
    time_t now = time(NULL);

    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        NodeEntry *prev = NULL;
        NodeEntry *curr = tabla_nodos.buckets[i];

        while (curr != NULL) {
            if (now - curr->last_seen > timeout_secs) {
                NodeEntry *to_delete = curr;
                
                // Desenlazar el nodo
                if (prev == NULL) {
                    tabla_nodos.buckets[i] = curr->next;
                } else {
                    prev->next = curr->next;
                }
                
                curr = curr->next;
                free(to_delete);
            } else {
                prev = curr;
                curr = curr->next;
            }
        }
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
}


void tabla_nodos_destroy() {
    pthread_mutex_lock(&tabla_nodos.mutex);
    for (int i = 0; i < TAM_TABLA_CONN; i++) {
        NodeEntry *next;
        while (tabla_nodos.buckets[i] != NULL) {
            next = tabla_nodos.buckets[i]->next;
            free(tabla_nodos.buckets[i]);
            tabla_nodos.buckets[i] = next;
        }
    }
    pthread_mutex_unlock(&tabla_nodos.mutex);
    pthread_mutex_destroy(&tabla_nodos.mutex);
    return;
}