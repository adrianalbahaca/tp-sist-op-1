# TP Final de Sistemas Operativos I

# 🖥️ Manejador de Recursos Distribuidos para HPC

**Trabajo Práctico — R-322 Sistemas Operativos I** **Licenciatura en Ciencias de la Computación**

Middleware distribuido para la gestión concurrente de recursos (CPUs, memoria, GPUs) en un clúster HPC simulado, combinando un servidor asíncrono en **C** (utilizando `epoll`) y un planificador lógico en **Erlang**.

---

## 📂 Estructura del Proyecto

La base de código está separada físicamente según el entorno de ejecución (C para infraestructura, Erlang para lógica) y dividida lógicamente según las responsabilidades del sistema.

```text
📦 hpc-resource-manager
├── 📜 Makefile                 # Reglas de compilación del proyecto
├── 📜 README.md                # Documentación principal
├── 🛠️ test_deadlock.sh         # Script de validación de condiciones de carrera
│
├── 📁 agente_c/                # Demonio de bajo nivel (Gestión de I/O y Estado)
│   ├── 📁 include/             # Cabeceras (.h)
│   │   ├── server.h            
│   │   ├── resource_manager.h  
│   │   ├── protocol.h          
│   │   └── discovery.h         
│   └── 📁 src/                 # Código fuente (.c)
│       ├── main.c              
│       ├── server.c            
│       ├── resource_manager.c  
│       ├── protocol.c          
│       └── discovery.c         
│
├── 📁 planificador_erl/        # Lógica de orquestación y concurrencia
│   └── 📁 src/                 # Código fuente (.erl)
│       ├── scheduler_main.erl  
│       ├── c_agent_client.erl  
│       ├── job_generator.erl   
│       └── deadlock_strategy.erl
│
└── 📁 docs/                    # Documentación formal
    └── 📄 informe_equipo.pdf   # Análisis de diseño y diagramas de secuencia
```

# Manejador de Recursos Distribuidos para HPC

**Trabajo Práctico — R-322 Sistemas Operativos I | LCC**

Middleware distribuido para la gestión concurrente de recursos (CPUs, memoria, GPUs) en un clúster HPC simulado. Combina un servidor asíncrono en **C** (utilizando `epoll`) y un planificador concurrente en **Erlang**.

---

## 📂 Árbol de Archivos y Asignación de Integrantes

La base de código está estructurada para permitir el desarrollo en paralelo. A continuación se detalla la responsabilidad de edición de cada archivo, asignada a los distintos miembros del equipo según su rol técnico.

### 📦 Configuración e Integración Continua

| Archivo / Directorio | Descripción Técnica | Editor Responsable |
| :--- | :--- | :--- |
| `Makefile` | Reglas de enlazado y compilación con flags estrictos (`-Wall -Wextra -pthread`). | **[Nombre Integrante 4]** *(Integración)* |
| `README.md` | Documentación técnica, dependencias e instrucciones de despliegue. | **[Nombre Integrante 4]** *(Integración)* |
| `test_deadlock.sh` | Script bash de validación de condiciones de carrera y parseo de logs. | **[Nombre Integrante 4]** *(Integración)* |

### 📁 `agente_c/` (Capa de Infraestructura y Estado)

| Archivo | Responsabilidad Técnica | Editor Responsable |
| :--- | :--- | :--- |
| `src/main.c` | Punto de entrada, inicialización de estructuras y orquestación del demonio. | **[Nombre Integrante 4]** *(Integración)* |
| `src/server.c`<br>`include/server.h` | Multiplexación `epoll`, I/O no bloqueante, manejo de descriptores TCP/UDP. | **[Nombre Integrante 1]** *(Comunicaciones)* |
| `src/discovery.c`<br>`include/discovery.h` | Emisión y recepción de datagramas UDP para descubrimiento topológico. | **[Nombre Integrante 1]** *(Comunicaciones)* |
| `src/resource_manager.c`<br>`include/resource_manager.h` | Gestión de memoria, actualización de recursos, tabla de jobs y colas FIFO. | **[Adrian Albahaca]** *(Gestor de Estado)* |
| `src/protocol.c`<br>`include/protocol.h` | Funciones de parseo de línea ASCII a estructuras de datos nativas en C. | **[Adrian Albahaca]** *(Gestor de Estado)* |

### 📁 `planificador_erl/` (Capa de Lógica y Coordinación)

| Archivo | Responsabilidad Técnica | Editor Responsable |
| :--- | :--- | :--- |
| `src/scheduler_main.erl` | Punto de entrada de la VM Erlang, supervisión y mantenimiento del estado global. | **[Nombre Integrante 3]** *(Planificador Erlang)* |
| `src/c_agent_client.erl` | Cliente TCP inter-proceso (`gen_tcp`) para enviar comandos al agente local. | **[Nombre Integrante 3]** *(Planificador Erlang)* |
| `src/job_generator.erl` | Generación heurística de requerimientos heterogéneos y selección de nodos. | **[Nombre Integrante 3]** *(Planificador Erlang)* |
| `src/deadlock_strategy.erl`| Implementación del algoritmo distribuido de prevención/detección de interbloqueos. | **[Nombre Integrante 3]** *(Planificador Erlang)* |

### 📁 `docs/` (Documentación Formal)

| Archivo | Descripción Técnica | Editor Responsable |
| :--- | :--- | :--- |
| `informe_equipo.pdf` | Justificación del algoritmo anti-deadlock, diagramas de secuencia de red. | **[Nombre Integrante 4]** *(Integración)* |

---

## ⚙️ Instrucciones de Despliegue

```bash
# 1. Compilación del binario C
make all

# 2. Inicialización del agente C local (Ejemplo: Puerto 8000)
./agente_c --port 8000

# 3. Compilación y ejecución del planificador Erlang
cd planificador_erl/src
erlc *.erl
erl -noshell -s scheduler_main start -s init stop

./agente 10.0.0.20 8000 2 4096 1