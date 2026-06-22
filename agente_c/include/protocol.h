#ifndef PROTOCOL_H
#define PROTOCOL_H
#include <stdbool.h>
#include "resource_types.h"

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

reserve_msg_t parse_reserve(const char* msg);

release_msg_t parse_release(const char* msg);

granted_msg_t parse_granted(const char* msg);

denied_msg_t parse_denied(const char* msg);

job_request_t parse_job_request(const char* buf);

void resource_list_destroy(resource_request_t *list);

#endif