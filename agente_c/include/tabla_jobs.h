#ifndef TABLA_JOBS_H
#define TABLA_JOBS_H

#define TAM_TABLA_JOBS 71 

#include "resource_types.h"
#include "server.h"
#include "tabla_conns.h"

#include <pthread.h>

/**
 * Se define un enum que determina sin un Allocation fue hecho por el Erlang local o por un nodo remoto
 */
typedef enum {
    REMOTE,
    LOCAL
} alloc_type_t;

typedef enum {
    RM_GRANTED,
    RM_QUEUED,
    RM_DENIED
} result_t;

/**
 * La tabla de Jobs almacena las solicitudes de un Job en una lista de Allocations, con la cantidad reservada y el tipo de alloc.
 * y una lista de OutRequest, solicitudes pendientes de agentes remotos
 * La tabla se implementa con una tabla hash, donde cada bucket tiene listas simplemente enlazadas para los 
 * Allocations y para los OutRequests. Usa el mismo job_id como clave de hasheo
 */
typedef struct Allocation {
    resource_t name; // Tipo de recurso
    int amount; // Cant. reservada
    alloc_type_t type;
    char ip[16]; // En caso de ser remoto, IP para enviar mensaje
    int job_id; // En caso de ser remoto, job_id de cual proviene
    result_t result; // Verifica si es que el recurso fue hecho bien o fue encolado
    connection_t *conn;
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

OutRequest* crear_outrequest(char* ip, connection_t *conn, resource_t type, int amount, char *msg);

Allocation* crear_allocation(resource_t type, int amount, alloc_type_t alloc_type, char *ip, int job_id, result_t result, connection_t *conn);

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
connection_t* tabla_jobs_get_conn(TablaJobs *j, int job_id, bool take_lock);

/**
 * Inserta un nuevo Job con el Allocation, o actualiza el Job con un nuevo Allocation
 */
void tabla_jobs_insertar_job(TablaJobs *t, Job* j);

/**
 * Remover un Job de la tabla de Jobs, ya sea porque ya se solicitó todos los requests o por una desconexión
 */
void tabla_jobs_remove(TablaJobs *j, int job_id, TablaConns *conns, int g_epfd, bool take_lock, bool take_lock_reserve);

/**
 * Recorre toda la tabla buscando los jobs de la conexión dada, elimina la lista de los OutRequest de cada Job y 
 * elimina el Job de la tabla. Se retorna una lista de todos los Allocations hechos con éxito para manejarlos en
 * Resource Manager
 */
Allocation* tabla_jobs_delete_by_conn(TablaJobs *j, connection_t *conn);

/**
 * Toma la solicitud pendiente con el IP enviado, y lo transfiere a la solicitud confirmada del Job dado
 * CUIDADO: Esta función no es thread-safe. Asume que se ha tomado tabla->lock antes de entrar acá
 */
bool tabla_jobs_confirmar(TablaJobs *j, const char *ip, int job_id);

/**
 * Verifica si todas las solicitudes externas pendientes fueron hechas
 */
bool tabla_jobs_verificar(TablaJobs *j, int job_id, bool take_lock);

/**
 * Obtener la lista de todos los pendientes en la tabla con esta conexión
 */
OutRequest* tabla_jobs_get_pendientes_by_conn(TablaJobs *j, connection_t *conn);

/**
 * Busca cada Job con la conexión dada, lo desenlaza de la tabla y crea una lista de todos los Jobs afectados, para
 * manejarlos en el Resource Manager
 */
Job *tabla_jobs_extract_by_remote_conn(TablaJobs *j, connection_t *conn);

void tabla_jobs_cambio_alloc(TablaJobs *j, int job_id, connection_t *conn, resource_t tipo, bool take_lock);

Job* tabla_jobs_buscar_por_id(TablaJobs* t, int job_id);

bool job_confirmar(Job* job, char *ip, int job_id);

Job* tabla_jobs_extract_by_id(TablaJobs *t, int job_id);

/**
 * Se define la función necesaria de forma externa
 */
extern void release_resource(resource_t tipo, int amount, bool take_lock);

#endif
