#ifndef PROTOCOL_H
#define PROTOCOL_H
#include <stdbool.h>
#include "resource_types.h"

/**
 * TODO: Documentar parsing.h completo
 */

/**
 * La lista de trabajos que tiene que hacer un elemento será una lista simplemente enlazada
 */
typedef struct resource_request_t {
    char ip[16];
    resource_t type;
    int amount;
    struct resource_request_t *next;
} resource_request_t;

typedef struct {
    bool valido;
    int job_id;
    resource_request_t *request_list;
} job_request_t;

/**
 * El RESERVE y RELEASE individuales deben ser estructuras aparte de esta lista
 */

typedef struct {
    bool valido;
    int job_id;
    resource_t type;
    int amount;
} reserve_msg_t;

typedef reserve_msg_t release_msg_t;

/**
 * Por cuestiones de implementación, GRANTED y DENIED tendrán sus propias estructuras
 */
typedef struct {
    bool valido;
    int job_id;
} granted_msg_t;

typedef granted_msg_t denied_msg_t;

typedef granted_msg_t job_denied_msg_t;

typedef granted_msg_t job_timeout_msg_t;

/**
 * Para los comandos como JOB_RELEASE, que se tienen que parsear, se crean sus mensajes acordes
 */

typedef granted_msg_t job_release_msg_t;

typedef granted_msg_t job_status_msg_t;

/**
 * Por último, el comando ANNOUNCE tiene su propio elemento
 */
typedef struct {
    bool valido;
    int puerto;
    int cpu;
    int mem;
    int gpu;
} announce_msg_t;

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * RESERVE <job_id> <resource_name> <amount>
 */
reserve_msg_t parse_reserve(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * RELEASE <job_id>
 */
release_msg_t parse_release(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * GRANTED <Job_id>
 */
granted_msg_t parse_granted(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * DENIED <job_id>
 */
denied_msg_t parse_denied(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * JOB_REQUEST <job_id> [@host:res:amount]
 */
job_request_t parse_job_request(const char* buf);

void resource_list_destroy(resource_request_t *list);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * JOB_RELEASE <job_id>
 */
job_release_msg_t parse_job_release(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * JOB_STATUS <job_id>
 */
job_status_msg_t parse_job_status(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * JOB_DENIED <job_id>
 */
job_denied_msg_t parse_job_denied(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * JOB_TIMEOUT <job_id>
 */
job_timeout_msg_t parse_job_timeout(const char* msg);

/**
 * Parsea el mensaje y retorna un objeto con los datos necesarios. El formato de este mensaje es:
 * ANNOUNCE <IP> <puerto> <recursos>
 */
announce_msg_t parse_announce(const char* msg);

#endif
