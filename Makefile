CC = gcc
CFLAGS = -Wall -Iagente_c/include -pthread
SRC = agente_c/src/resource_manager.c agente_c/src/protocol.c agente_c/src/server.c agente_c/src/main.c
TARGET = agente

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -f $(TARGET)