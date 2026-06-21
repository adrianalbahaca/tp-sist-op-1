#ifndef RESOURCE_MANAGER_H
#define RESOURCE_MANAGER_H

#include "../include/server.h"

/**
 * Inicializa la estructura gestor de recursos para el agente C local
 */
void manager_init(int cpu, int mem, int gpu);

/**
 * Destruye la estructura gestor de recursos, liberando memoria
 */
void manager_destroy();

/**
 * Ajusta el epoll de forma global para el gestor de recursos
 */
void manager_set_epoll(int epfd);

/**
 * Dentro de handle_tcp_read, luego de cambiar (\n) por (\0).
 * Debe decidir qué hacer con el mensaje, si RESERVE, RELEASE, etc.
 */
void process_message(connection_t *conn, char *msg);

/**
 * Dentro de handle_udp_read, por cada datagrama que llega al socket UDP.
 * Para agregarlo a los conocidos.
 */
void process_announce(const char *ip_sender, const char *message);


/**
 * Llamar antes de destruir el socket
 */
void process_disconnect(connection_t *conn);

/**
 * Se llama cuando un connect_remote_node finaliza con éxito.
 * Acá el Gestor de Estado ya puede llamar a enqueue_write con sus peticiones.
 */
void process_connection_ready(connection_t *conn);

/**
 * Se llama si connect_remote_node falló (ej. el nodo B estaba apagado).
 * El Gestor de Estado debe abortar su plan y quizás buscar otro nodo.
 */
void process_connection_failed(connection_t *conn);


#endif