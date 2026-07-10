CC = gcc
CFLAGS = -Wall -Iagente_c/include -lpthread -g
SRC = agente_c/src/resource_manager.c agente_c/src/parsing.c agente_c/src/server.c \
	agente_c/src/main.c agente_c/src/pending_request.c agente_c/src/tabla_jobs.c \
	agente_c/src/tabla_conns.c agente_c/src/tabla_node_entry.c
TARGET = agente
PLANIF = planificador.log

ERLC       = erlc
ERLC_FLAGS = -W
ERL_SRC    = planificador_erl/c_agent_client.erl
ERL_FILE   = c_agent_client.beam
ERL_BEAM   = $(ERL_SRC:.erl=.beam)

# Parámetros por defecto para la ejecución dinámica del agente C
IP ?= 192.168.146.50
PORT ?= 8000
CPU ?= 2
MEM ?= 4096
GPU ?= 1

.PHONY: all clean run-c run-erl

# Compilar todo
all: $(TARGET) $(ERL_BEAM)

# Compilado del agente C
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

# Compilado del cliente en Erlang
%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<

# Ejecución del agente C con inyección de variables
run-c: $(TARGET)
	./$(TARGET) $(IP) $(PORT) $(CPU) $(MEM) $(GPU)

# Ejecución del cliente Erlang en una sola línea
run-erl: $(ERL_BEAM)
	erl -pa planificador_erl -noshell -s c_agent_client start

clean:
	rm -rf $(TARGET) $(ERL_FILE) $(PLANIF)


# Comandos a ejecutar en cada terminal para correr el programa en una pc
# Me parece que la ip que tenés que usar para correrlo es la que devuelve hostname -I
# make run-c IP=192.168.1.50 PORT=8000 CPU=8 MEM=16384 GPU=2
# make run-erl
