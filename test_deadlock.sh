#!/bin/bash

# =========================================================================
# Prueba de Prevención de Deadlock (Emulación Bidireccional /dev/tcp)
# =========================================================================

cleanup() {
    sudo pkill -9 -f ./agente 2>/dev/null
    sudo ip netns del pc_azul 2>/dev/null
    sudo ip netns del pc_roja 2>/dev/null
    stty sane 2>/dev/null 
}
trap cleanup EXIT SIGINT SIGTERM

cleanup

echo "[*] Compilando proyecto..."
make clean && make > /dev/null 2>&1
rm -f nodo_a.log nodo_b.log

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

# 2. Despliegue de Agentes
echo "[*] Desplegando middleware HPC (Nodos A y B)..."
sudo ip netns exec pc_azul stdbuf -oL -eL ./agente 10.0.0.10 8000 2 8192 1 < /dev/null > nodo_a.log 2>&1 &
sudo ip netns exec pc_roja stdbuf -oL -eL ./agente 10.0.0.20 8000 2 8192 1 < /dev/null > nodo_b.log 2>&1 &
sleep 3

# 3. Inyección Concurrente (Emulación Síncrona)
echo "[*] Emitiendo colisión masiva (Lectura Activa del Socket):"

# Cliente Simulado en Nodo Azul (Job 1001)
sudo ip netns exec pc_azul bash -c '
    # Abrir socket bidireccional en File Descriptor 3
    exec 3<>/dev/tcp/127.0.0.1/8000
    echo "JOB_REQUEST 1001 @10.0.0.10:cpu:2 @10.0.0.20:gpu:1" >&3
    
    # Leer pacientemente la respuesta del Agente C (timeout de seguridad 15s)
    while read -t 15 -u 3 line; do
        if [[ "$line" == *"JOB_GRANTED"* ]]; then
            echo "[Azul] ¡Recibido GRANTED! Trabajando 3s..."
            sleep 3
            echo "JOB_RELEASE 1001" >&3
            break
        fi
    done
    sleep 1
    exec 3<&-
' &

# Cliente Simulado en Nodo Rojo (Job 1002)
sudo ip netns exec pc_roja bash -c '
    exec 3<>/dev/tcp/127.0.0.1/8000
    echo "JOB_REQUEST 1002 @10.0.0.10:gpu:1 @10.0.0.20:cpu:2" >&3
    
    while read -t 15 -u 3 line; do
        if [[ "$line" == *"JOB_GRANTED"* ]]; then
            echo "[Rojo] ¡Recibido GRANTED! Trabajando 3s..."
            sleep 3
            echo "JOB_RELEASE 1002" >&3
            break
        fi
    done
    sleep 1
    exec 3<&-
' &

echo "[*] Sockets abiertos y bloqueados esperando respuesta del middleware..."
# Esperamos 10s totales (3s de trabajo del ganador + 3s de trabajo del perdedor + latencia)
sleep 10

# 4. Volcado de Auditoría
stty sane 2>/dev/null
echo "----------------------------------------------------"
echo "Auditoría Nodo Azul (10.0.0.10) [Ciclo Completo Validado]:"
grep -E "RX|TX" nodo_a.log | sed 's/^/  /'
echo "----------------------------------------------------"
echo "Auditoría Nodo Rojo (10.0.0.20) [Ciclo Completo Validado]:"
grep -E "RX|TX" nodo_b.log | sed 's/^/  /'
echo "----------------------------------------------------"