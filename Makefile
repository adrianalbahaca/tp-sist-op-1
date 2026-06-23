CC = gcc
CFLAGS = -Wall -Iagente_c/include -pthread -g
SRC = agente_c/src/resource_manager.c agente_c/src/protocol.c agente_c/src/server.c agente_c/src/main.c
TARGET = agente

ERLC       = erlc
ERLC_FLAGS = -W
ERL_SRC    = planificador_erl/c_agent_client.erl
ERL_BEAM   = $(ERL_SRC:.erl=.beam)

.PHONY: all clean run-c run-erl

# Compilar todo
all: $(TARGET) $(ERL_BEAM)

# Compilado del agente C
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

# Compilado del cliente en Erlang
%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) $<

# Ejecución de los programas compilados
run-c: $(TARGET)
	./$(TARGET) 10.0.0.10 8100 4 8192 1

run-erl: $(ERL_BEAM)
	erl -pa planificador_erl -noshell -s c_agent_client start

clean:
	rm -f $(TARGET) $(ERL_BEAM)