#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include "../include/server.h"
#include "../include/resource_manager.h"

typedef struct {
    char ip[16];
    int port;
    int cpu;
    int mem;
    int gpu;
} config_t;

extern void tabla_nodos_purge(int timeout_secs);

/**
 * Hilo Auxiliar: Emisión de estado UDP y purga de nodos caídos.
 * Ejecuta el ciclo de descubrimiento continuo según el protocolo 5.3.
 */
void *broadcast_loop(void *arg) {
    config_t *cfg = (config_t *)arg;
    char announce_msg[256];
    
    snprintf(announce_msg, sizeof(announce_msg), "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n",
             cfg->port, cfg->cpu, cfg->mem, cfg->gpu);

    while (1) {
        send_udp_broadcast(UDP_DISCOVERY_PORT, announce_msg);
        tabla_nodos_purge(15);
        sleep(5);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    // Validación estricta de argumentos
    if (argc != 6) {
        fprintf(stderr, "Uso: %s <IP_PUBLICA> <PUERTO> <CPU> <MEM> <GPU>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    config_t cfg;
    strncpy(cfg.ip, argv[1], sizeof(cfg.ip) - 1);
    cfg.ip[sizeof(cfg.ip) - 1] = '\0';
    cfg.port = atoi(argv[2]);
    cfg.cpu = atoi(argv[3]);
    cfg.mem = atoi(argv[4]);
    cfg.gpu = atoi(argv[5]);

    // Inicialización de Memoria y Gestor de Recursos
    manager_init(cfg.cpu, cfg.mem, cfg.gpu);
    manager_set_ip(cfg.ip);

    // Despliegue de Sockets e inicialización de epoll
    int epfd_global;
    if (init_server_sockets(cfg.ip, cfg.port, &epfd_global) < 0) {
        fprintf(stderr, "Fallo crítico en inicialización de capa de red.\n");
        manager_destroy();
        exit(EXIT_FAILURE);
    }
    manager_set_epoll(epfd_global);

    // Emisión del primer datagrama UDP de descubrimiento
    char announce_msg[256];
    snprintf(announce_msg, sizeof(announce_msg), "ANNOUNCE %d cpu:%d mem:%d gpu:%d\n", 
             cfg.port, cfg.cpu, cfg.mem, cfg.gpu);
    send_udp_broadcast(UDP_DISCOVERY_PORT, announce_msg);
    
    // Retardo estructural para recibir topología preexistente antes de operar
    sleep(2);

    // Despliegue del pool de hilos de atención I/O
    pthread_t threads_pool[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
        if (pthread_create(&threads_pool[i], NULL, worker_thread_loop, &epfd_global) != 0) {
            perror("Fallo en pthread_create (I/O Workers)");
            exit(EXIT_FAILURE);
        }
    }

    // Despliegue del hilo de broadcast UDP
    pthread_t thread_bc;
    if (pthread_create(&thread_bc, NULL, broadcast_loop, &cfg) != 0) {
        perror("Fallo en pthread_create (Broadcast)");
        exit(EXIT_FAILURE);
    }

    // Bloqueo de ejecución del hilo principal
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads_pool[i], NULL);
    }

    // Destrucción estructurada en caso de salida del loop principal
    manager_destroy();
    return 0;
}