#include "../include/resource_types.h"
#include "../include/protocol.h"
#include <string.h>
#include <stdio.h>

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
        result.type = RESOURCE_CPU
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