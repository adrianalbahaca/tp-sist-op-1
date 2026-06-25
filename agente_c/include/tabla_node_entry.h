#ifndef TABLA_NODE_ENTRY_H
#define TABLA_NODE_ENTRY_H

#include <time.h>
#include <pthread.h>

// Se selecciona un numero primo chico por cuestiones de optimización
#define TAM_TABLA_CONN 71

typedef struct NodeEntry {
    char ip[16];
    int puerto;
    int cpu_disp, mem_disp, gpu_disp;
    time_t last_seen;
    struct NodeEntry *next;
} NodeEntry;

typedef struct {
    NodeEntry *buckets[TAM_TABLA_CONN];
    pthread_mutex_t mutex;
} TablaNodos;

void tabla_nodos_init();

void tabla_nodos_insert_or_update(const char *ip, int puerto, int cpu, int mem, int gpu);

/**
 * Busca el puerto asociado a una IP conocida. Devuelve -1 si no se encontró.
 */
int tabla_nodos_get_puerto(const char *ip);

/**
 * Barre la tabla de nodos eliminando aquellos cuyo último anuncio (last_seen)
 * supere el tiempo de expiración (timeout_secs). Requerido por el protocolo de 15 segundos.
 */
void tabla_nodos_purge(int timeout_secs);

void tabla_nodos_destroy();

extern TablaNodos tabla_nodos;

#endif