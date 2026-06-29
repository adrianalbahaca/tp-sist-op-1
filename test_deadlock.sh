#!/bin/bash

# =========================================================================
# Prueba de Prevención de Deadlock (con clientes Erlang reales)
# =========================================================================

cleanup() {
    sudo pkill -9 -f ./agente 2>/dev/null
    sudo pkill -9 -f beam 2>/dev/null
    sudo ip netns del pc_azul 2>/dev/null
    sudo ip netns del pc_roja 2>/dev/null
    stty sane 2>/dev/null
}
trap cleanup EXIT SIGINT SIGTERM

cleanup

echo "[*] Compilando proyecto C..."
make clean && make > /dev/null 2>&1
rm -f nodo_a.log nodo_b.log erlang_azul.log erlang_roja.log

echo "[*] Compilando módulo Erlang..."
cd planificador_erl && erlc c_agent_client.erl && cd ..

echo "[*] Levantando topología virtual aislada..."
sudo ip netns add pc_azul
sudo ip netns add pc_roja
sudo ip link add veth_azul type veth peer name veth_roja
sudo ip link set veth_azul netns pc_azul
sudo ip link set veth_roja netns pc_roja

sudo ip netns exec pc_azul ip addr add 10.0.0.10/24 dev veth_azul
sudo ip netns exec pc_azul ip link set veth_azul up
sudo ip netns exec pc_azul ip link set lo up
sudo ip netns exec pc_azul ip route add 255.255.255.255 dev veth_azul

sudo ip netns exec pc_roja ip addr add 10.0.0.20/24 dev veth_roja
sudo ip netns exec pc_roja ip link set veth_roja up
sudo ip netns exec pc_roja ip link set lo up
sudo ip netns exec pc_roja ip route add 255.255.255.255 dev veth_roja
sleep 1

# 2. Despliegue de Agentes C
echo "[*] Desplegando middleware HPC (Nodos Azul y Rojo)..."
sudo ip netns exec pc_azul stdbuf -oL -eL ./agente 10.0.0.10 8000 4 8192 1 < /dev/null > nodo_a.log 2>&1 &
PID_AGENTE_AZUL=$!
sudo ip netns exec pc_roja stdbuf -oL -eL ./agente 10.0.0.20 8000 4 8192 1 < /dev/null > nodo_b.log 2>&1 &
PID_AGENTE_ROJA=$!
sleep 3

# Verificación temprana: ambos agentes C deben seguir vivos antes de lanzar Erlang
if ! kill -0 $PID_AGENTE_AZUL 2>/dev/null; then
    echo "[!] ERROR: el agente C de Azul murió antes de arrancar Erlang. Ver nodo_a.log:"
    cat nodo_a.log
    exit 1
fi
if ! kill -0 $PID_AGENTE_ROJA 2>/dev/null; then
    echo "[!] ERROR: el agente C de Rojo murió antes de arrancar Erlang. Ver nodo_b.log:"
    cat nodo_b.log
    exit 1
fi

# 3. Lanzamiento de los clientes Erlang reales, uno por nodo, en paralelo
echo "[*] Lanzando clientes Erlang (test_deadlock1 / test_deadlock2)..."

cd planificador_erl

sudo ip netns exec pc_azul erl -noshell -pa . \
    -eval "c_agent_client:test_deadlock1(), init:stop()." \
    > ../erlang_azul.log 2>&1 &
PID_AZUL=$!

sudo ip netns exec pc_roja erl -noshell -pa . \
    -eval "c_agent_client:test_deadlock2(), init:stop()." \
    > ../erlang_roja.log 2>&1 &
PID_ROJA=$!

cd ..

echo "[*] Esperando resultado (timeout máximo: 35s)..."

# Esperamos un tiempo generoso, mayor a cualquier timeout interno de Erlang (30s)
TIMEOUT=35
ELAPSED=0
while kill -0 $PID_AZUL 2>/dev/null || kill -0 $PID_ROJA 2>/dev/null; do
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "[!] Timeout del script alcanzado, forzando cierre de procesos colgados."
        break
    fi
done

# Por si alguno quedó colgado más allá del timeout interno
kill -9 $PID_AZUL $PID_ROJA 2>/dev/null

# Verificación: ¿los agentes C seguían vivos al final de la espera?
echo "----------------------------------------------------"
if kill -0 $PID_AGENTE_AZUL 2>/dev/null; then
    echo "[OK] Agente C de Azul seguía vivo al finalizar."
else
    echo "[!] Agente C de Azul NO estaba vivo al finalizar — posible crash silencioso."
fi
if kill -0 $PID_AGENTE_ROJA 2>/dev/null; then
    echo "[OK] Agente C de Rojo seguía vivo al finalizar."
else
    echo "[!] Agente C de Rojo NO estaba vivo al finalizar — posible crash silencioso."
fi

# 4. Volcado de Auditoría
stty sane 2>/dev/null
echo "----------------------------------------------------"
echo "Salida cliente Erlang AZUL (orden: Azul, Rojo):"
cat erlang_azul.log | sed 's/^/  /'
echo "----------------------------------------------------"
echo "Salida cliente Erlang ROJO (orden: Rojo, Azul):"
cat erlang_roja.log | sed 's/^/  /'
echo "----------------------------------------------------"
echo "Auditoría Nodo Azul (10.0.0.10) [RX/TX]:"
grep -E "RX|TX" nodo_a.log | sed 's/^/  /'
echo "----------------------------------------------------"
echo "Auditoría Nodo Rojo (10.0.0.20) [RX/TX]:"
grep -E "RX|TX" nodo_b.log | sed 's/^/  /'
echo "----------------------------------------------------"

# 5. Veredicto automático: verificamos POSITIVAMENTE que ambos llegaron a destino
AZUL_OK=false
ROJA_OK=false

if grep -qi "trabajando\|recibió\|RESUELTO" erlang_azul.log; then
    AZUL_OK=true
fi

if grep -qi "trabajando\|recibió\|RESUELTO" erlang_roja.log; then
    ROJA_OK=true
fi

echo ""
if [ "$AZUL_OK" = true ] && [ "$ROJA_OK" = true ]; then
    echo "[RESULTADO] ✓ Ambos clientes (Azul y Rojo) recibieron confirmación — deadlock evitado correctamente."
    exit 0
else
    echo "[RESULTADO] ⚠ Al menos un cliente no llegó a confirmar respuesta — posible deadlock NO resuelto."
    [ "$AZUL_OK" = false ] && echo "    -> Azul: SIN confirmación"
    [ "$ROJA_OK" = false ] && echo "    -> Rojo: SIN confirmación"
    exit 1
fi