CC = gcc
CFLAGS = -Wall -Iagente_c/include -pthread
SRC = agente_c/src/resource_manager.c agente_c/src/protocol.c agente_c/src/server.c agente_c/src/main.c
TARGET = agente

ERLC       = erlc
ERLC_FLAGS = -W
ERL_SRC    = planificador_erl/c_agent_client.erl
ERL_BEAM   = $(ERL_SRC:.erl=.beam)   # cliente.beam protocolo.beam

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
run-c: $(C_BIN)
	./$(TARGET)

run-erl: $(ERL_BEAM)
	erl -noshell -s c_agent_client start

clean:
	rm -f $(TARGET) $(ERL_BEAM)