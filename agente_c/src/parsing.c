#include "resource_types.h"
#include "parsing.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/**
 * =================================================================================================
 * Código de la lista de requests
 * =================================================================================================
 */

 /**
  * Inserta el recurso en la lista
  */
static void resource_list_insert(resource_request_t **list, char ip[16], resource_t type, int amount) {
    resource_request_t *resource = malloc(sizeof(resource_request_t));
    assert(resource != NULL);
    resource->amount = amount;
    resource->type = type;
    strncpy(resource->ip, ip, sizeof(resource->ip)-1);
    resource->ip[sizeof(resource->ip) - 1] = '\0';
    resource->next = NULL;

    if (*list == NULL) {
        *list = resource;
    } else {
        resource_request_t *curr = *list;
        while (curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = resource;
    }
}

/**
 * Destruye la lista de recursos
 */
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

/**
 * Parsea los comandos que inician con RESERVE
 */
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

/**
 * Parsea los comandos que inician con RELEASE
 */
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

/**
 * Parsea los comandos que inician con GRANTED
 */
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

/**
 * Parsea los comandos que inician con DENIED
 */
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

/**
 * Parsea los comandos que inician con JOB_REQUEST
 */
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
    result.request_list = NULL;

    // Descartar los primeros 2 tokens ("JOB_REQUEST" y "<job_id>")
    char* saveptr;
    char* token = strtok_r(msg, " ", &saveptr);
    token = strtok_r(NULL, " ", &saveptr);

    char ip[16], rec[7];
    int c_amt;

    // Iterar sobre cada bloque contiguo separado por espacios
    while ((token = strtok_r(NULL, " ", &saveptr)) != NULL) {
        // Máscara exacta para el formato: @10.0.0.10:cpu:2:mem:10:gpu:1
        // Un mismo host puede pedir varios tipos de recurso a la vez (cpu, mem,
        // gpu), así que hay que recorrer TODOS los pares ":tipo:cantidad" del
        // token, no sólo el primero.
        int ip_len = 0;
        if (sscanf(token, "@%15[^:]%n", ip, &ip_len) != 1) {
            resource_list_destroy(result.request_list);
            free(msg);
            return result; // Fallo de parseo léxico
        }

        const char *rest = token + ip_len;
        if (*rest != ':') {
            resource_list_destroy(result.request_list);
            free(msg);
            return result; // Sin ningún par tipo:cantidad
        }

        while (*rest == ':') {
            int consumed = 0;
            if (sscanf(rest, ":%7[^:]:%d%n", rec, &c_amt, &consumed) != 2) {
                resource_list_destroy(result.request_list);
                free(msg);
                return result;
            }

            resource_t type;
            if (strcmp(rec, "cpu") == 0) {
                type = RESOURCE_CPU;
            }
            else if (strcmp(rec, "mem") == 0) {
                type = RESOURCE_MEM;
            }
            else if (strcmp(rec, "gpu") == 0) {
                type = RESOURCE_GPU;
            }
            else {
                resource_list_destroy(result.request_list);
                free(msg);
                return result;
            }
            if (c_amt > 0)
                resource_list_insert(&result.request_list, ip, type, c_amt);

            rest += consumed;
        }
    }

    result.valido = true;
    free(msg);
    return result;
}

/**
 * Parsea los comandos que inician con JOB_RELEASE
 */
job_release_msg_t parse_job_release(const char* msg) {
    job_status_msg_t result;
    result.valido = false;

    // Parsear mensaje
    int job_id;
    if (sscanf(msg, "JOB_RELEASE %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;

    return result;
}

/**
 * Parsea los comandos que inician con JOB_STATUS
 */
job_status_msg_t parse_job_status(const char* msg) {
    job_status_msg_t result;
    result.valido = false;

    // Parsear mensaje
    int job_id;
    if (sscanf(msg, "JOB_STATUS %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;

    return result;
}

/**
 * Parsea los comandos que inician con JOB_DENIED
 */
job_denied_msg_t parse_job_denied(const char* msg) {
    job_denied_msg_t result;
    result.valido = false;

    int job_id;
    if (sscanf(msg, "JOB_DENIED %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;
    return result;
}

/**
 * Parsea los comandos que inician con JOB_TIMEOUT
 */
job_timeout_msg_t parse_job_timeout(const char* msg) {
    job_timeout_msg_t result;
    result.valido = false;

    int job_id;
    if(sscanf(msg, "JOB_TIMEOUT %d", &job_id) != 1) {
        return result;
    }

    result.job_id = job_id;
    result.valido = true;
    return result;
}

/**
 * Este es el parseo del comando ANNOUNCE
 * Este código permite recibir un ANNOUNCE cualquiera sin importar el orden de los recursos que recibe
 */
announce_msg_t parse_announce(const char* msg) {
    
    char temp[256];
    strncpy(temp, msg, sizeof(temp) - 1);
    temp[sizeof(temp) - 1] = '\0';

    announce_msg_t result;
    result.valido = false;

    int puerto = 0, cpu = -1, mem = -1, gpu = -1;
    char* saveptr;

    char* token = strtok_r(temp, " ", &saveptr);
    if (token == NULL || strcmp(token, "ANNOUNCE") != 0) {
        fprintf(stderr, "ANNOUNCE mal formado: (token [%s])%s\n",token, msg);
        return result;
    }

    token = strtok_r(NULL, " ", &saveptr);
    if (token == NULL || sscanf(token, "%d", &puerto) != 1) {
        fprintf(stderr, "ANNOUNCE mal formado (puerto inválido): %s\n", msg);
        return result;
    }
    int i = 0;
    while ((token = strtok_r(NULL, " ", &saveptr)) != NULL && i < 3) {
        if (sscanf(token, "cpu:%d", &cpu) == 1) {
            i++;
            continue;
        }
        if (sscanf(token, "mem:%d", &mem) == 1){
            i++;
            continue;
        }
        if (sscanf(token, "gpu:%d", &gpu) == 1){
            i++;
            continue;
        }
        
        fprintf(stderr, "cpu%d gpu%d mem%d - [%s] - ", cpu, gpu, mem, token);
        fprintf(stderr, "ANNOUNCE mal formado (componente): [%s]\n", msg);
        return result;
    }

    if (cpu == -1 || mem == -1 || gpu == -1) {
        fprintf(stderr, "%d %d %d", cpu, gpu, mem);
        fprintf(stderr, "ANNOUNCE mal formado - faltan: %s\n", msg);
        return result;
    }

    result.cpu = cpu;
    result.mem = mem;
    result.gpu = gpu;
    result.puerto = puerto;
    result.valido = true;

    return result;
}
