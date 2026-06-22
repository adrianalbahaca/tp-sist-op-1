#include "../include/resource_types.h"
#include "../include/protocol.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/**
 * =================================================================================================
 * Código de la lista de requests
 * =================================================================================================
 */
static void resource_list_insert(resource_request_t **list, char ip[16], resource_t type, int amount) {
    resource_request_t *resource = malloc(sizeof(resource_request_t));
    resource->amount = amount;
    resource->type = type;
    strncpy(resource->ip, ip, sizeof(resource->ip)-1);
    resource->ip[sizeof(resource->ip) - 1] = '\0';

    if (list == NULL) {
        resource->next = NULL;
    }
    else {
        resource->next = *list;
    }

    *list = resource;

    return;
}

void resource_list_destroy(resource_request_t *list) {
    resource_request_t *next;

    while (list != NULL) {
        next = list->next;
        // free(list->ip);
        free(list);
        list = next;
    }

    list = NULL;
    return;
}

reserve_msg_t parse_reserve(const char* msg) {
    reserve_msg_t result;
    result.valido = false;

    /**
     * El formato del comando recibido debe ser RESERVE <job_id> <resource_name> <amount>
     */
    char type[16];
    int job_id, amount;
    if (sscanf(msg, "RESERVE %d %15s %d", &job_id, type, &amount) != 3) {
        return result;
    }

    result.amount = amount;
    result.job_id = job_id;

    if (strcmp(type, "cpu") == 0) {
        result.type = RESOURCE_CPU;
    }
    else if (strcmp(type, "mem") == 0) {
        result.type = RESOURCE_MEM;
    }
    else if (strcmp(type, "gpu") == 0) {
        result.type = RESOURCE_GPU;
    }
    else {
        return result;
    }

    result.valido = true;
    return result;
}

release_msg_t parse_release(const char* msg) {
    release_msg_t result;
    result.valido = false;

    /**
     * El formato del comando recibido debe ser RELEASE <job_id> <resource_name> <amount>
     */

    char type[16];
    int job_id, amount;
    if (sscanf(msg, "RELEASE %d %15s %d", &job_id, type, &amount) != 3) {
        return result;
    }

    result.job_id = job_id;
    result.amount = amount;

    if (strcmp("cpu", type) == 0) {
        result.type = RESOURCE_CPU;
    }
    else if (strcmp("mem", type) == 0) {
        result.type = RESOURCE_MEM;
    }
    else if (strcmp("gpu", type) == 0) {
        result.type = RESOURCE_GPU;
    }
    else {
        return result;
    }

    result.valido = true;
    return result;
}

granted_msg_t parse_granted(const char* msg) {
    granted_msg_t result;
    result.valido = false;

    int job_id;
    if (sscanf(msg, "GRANTED %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;

    return result;
}

denied_msg_t parse_denied(const char* msg) {
    denied_msg_t result;
    result.valido = false;

    int job_id;
    if(sscanf(msg, "DENIED %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;

    return result;
}

job_request_t parse_job_request(const char* buf) {
    job_request_t result;
    result.valido = false;

    char* msg = strdup(buf);
    // Obtener job_id
    int job_id;
    if (sscanf(msg, "JOB_REQUEST %d", &job_id) != 1) {
        free(msg);
        return result;
    }
    result.job_id = job_id;
    
    // Empezar compilación de elementos
    result.request_list = NULL; // La lista inicia vacía

    // Descartar los primeros 2 comandos
    char* saveptr;
    char* token = strtok_r(msg, " ", &saveptr);
    token = strtok_r(NULL, " ", &saveptr);

    // Luego, iterar sobre cada uno
    char ip[16], recurso[16];
    int amount;
    resource_t type;

    while ((token = strtok_r(NULL, " ", &saveptr)) != NULL) {
        if (sscanf(token, "@%15[^:]:%15[^:]:%d", ip, recurso, &amount) != 3) {
            resource_list_destroy(result.request_list);
            free(msg);
            return result;
        }
        else {
            if (strcmp(recurso, "cpu") == 0) {
                type = RESOURCE_CPU;
            }
            else if (strcmp(recurso, "mem") == 0) {
                type = RESOURCE_MEM;
            }
            else if (strcmp(recurso, "gpu") == 0) {
                type = RESOURCE_GPU;
            }
            else {
                resource_list_destroy(result.request_list);
                free(msg);
                return result;
            }

            resource_list_insert(&result.request_list, ip, type, amount);
        }
    }

    // Si salió sin problemas, se debe de retornar el resultado adecuadamente
    result.valido = true;
    free(msg);
    return result;

}