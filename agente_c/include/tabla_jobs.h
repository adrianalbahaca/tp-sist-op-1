#ifndef TABLA_JOBS_H
#define TABLA_JOBS_H

#define TAM_TABLA_JOBS 71 

#include "resource_types.h"
#include "server.h"
#include "tabla_conns.h"

#include <pthread.h>

typedef enum {
    REMOTE,
    LOCAL
} alloc_type_t;

/**
 * La tabla de Jobs almacena las solicitudes de un Job en una lista de Allocations, con la cantidad acorde a reservar
 * y una lista de OutRequest, solicitudes pendientes de agentes remotos y local
 * La tabla se implementa con una tabla hash, donde cada bucket tiene una lista simplemente enlazada para los 
 * Allocations y una cola hecha con una lista simplemente enlazada para los OutRequests. Usa el mismo job_id como
 * clave de hasheo
 */
typedef struct Allocation {
    resource_t name; // Tipo de recurso
    int amount; // Cant. reservada
    alloc_type_t type;
    char ip[16]; // En caso de ser remoto, IP para enviar mensaje
    int job_id; // En caso de ser remoto, job_id de cual proviene
    struct Allocation *sig; // Siguiente reservación
} Allocation;

typedef struct OutReq {
    char ip[15]; // IP del nodo remoto a solicitar el Job
    connection_t *conn; // Conexión proveniente de la solicitud del Job
    resource_t tipo; // Tipo de recurso
    int amount; // Cant. a reservar
    char msg[BUFF_SIZE]; // Mensaje a enviar una vez se haga la conexión remota
    struct OutReq *next; // Siguiente solicitud
} OutRequest;

typedef struct Job {
    int job_id; // ID del Job
    connection_t *conn; // Conexión que hizo la solicitud

    Allocation *confirmadas; // Recursos con GRANTED hecho
    OutRequest *pendientes; // Recursos con GRANTED pendiente

    struct Job *sig; // Siguiente Job
} Job;

typedef struct {
    Job* tabla_jobs[TAM_TABLA_JOBS]; // Array de lista de Jobs
    pthread_mutex_t lock; // Lock para la tabla completa
} TablaJobs;

/**
 * Inicialización de la tabla interna para el Resource Manager
 */
void tabla_jobs_init(TablaJobs *j);

/**
 * Destrucción de la tabla interna del Resource Manager de cerrarse el servidor
 */
void tabla_jobs_destroy(TablaJobs *j);

/**
 * Busca un Job usando su Job ID
 */
bool tabla_jobs_get_id(TablaJobs *t, int job_id);

/**
 * Busca el conn original asociado a un job_id, sin importar el bucket.
 * Devuelve NULL si no se encuentra.
 */
connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id);

/**
 * Inserta un nuevo Job con el Allocation, o actualiza el Job con un nuevo Allocation
 */
void tabla_jobs_insert(TablaJobs *j, connection_t *conn, int job_id, resource_t type, int amount, int max_amount, char* ip, connection_t* remote, const char *msg, const char *g_ip);

/**
 * Remover un Job de la tabla de Jobs, ya sea porque ya se solicitó todos los requests o por una desconexión
 */
void tabla_jobs_remove(TablaJobs *j, int job_id, TablaConns *conns, int g_epfd);

/**
 * Recorre toda la tabla buscando jobs de la conexión dada, libera sus recursos
 * (vía release_resource, que SÍ toma su propio lock) y elimina los jobs.
 * NO se llama con j->mutex tomado, porque release_resource necesita tomar
 * el mutex de cada Recurso y no queremos anidar locks innecesariamente.
 */
Allocation* tabla_jobs_delete_by_conn(TablaJobs *j, connection_t *conn);

/**
 * Toma la solicitud pendiente con el IP enviado, y lo transfiere a la solicitud confirmada del Job dado
 * CUIDADO: Esta función no es thread-safe. Asume que se ha tomado tabla->lock antes de entrar acá
 */
bool tabla_jobs_confirmar(TablaJobs *j, const char *ip, int job_id);

/**
 * Obtener la lista de todos los pendientes en la tabla con esta conexión
 */
OutRequest* tabla_jobs_get_pendientes_by_conn(TablaJobs *j, connection_t *conn);

Job *tabla_jobs_extract_by_remote_conn(TablaJobs *j, connection_t *conn);

/**
 * Declaraciones externas para usar en la librería
 */

extern void release_resource(resource_t tipo, int amount);

#endif
