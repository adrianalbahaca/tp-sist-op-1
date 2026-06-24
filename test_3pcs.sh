#!/bin/bash
# =========================================================================
# Simulación de 3 "PCs" en una LAN virtual (netns + bridge) para depurar
# el flujo Erlang local -> Agente C local -> Agentes C remotos.
#
# Topología:
#   br0 (bridge, sin IP, solo para que el broadcast UDP llegue a las 3)
#    ├── pc_azul  10.0.0.10  -> agente C + planificador Erlang (LOCAL)
#    ├── pc_roja  10.0.0.20  -> agente C (remoto)
#    └── pc_verde 10.0.0.30  -> agente C (remoto)
#
# Usalo desde la raíz del repo (TP-Final/), donde está el Makefile.
#
# IMPORTANTE: NO correr este script con "sudo bash ./test_3pcs.sh".
# Corrarlo como tu usuario normal: ./test_3pcs.sh
# El script pide sudo SOLO para los comandos de red (ip netns / ip link).
# Si se corre todo bajo sudo, "make all" hereda el PATH restringido de sudo
# (secure_path) y puede no encontrar erlc/erl aunque "which erlc" sí los vea
# en tu shell normal. Por eso separamos: compilación sin sudo, red con sudo.
# =========================================================================

set -u

if [ "$EUID" -eq 0 ]; then
    echo "[!] No corras este script con sudo/como root directamente."
    echo "    Corrélo como tu usuario normal: ./test_3pcs.sh"
    echo "    (el script va a pedir sudo solo donde lo necesita, para 'ip netns'/'ip link')"
    exit 1
fi

# Verificación temprana: confirmar que sudo SÍ puede encontrar lo que necesita
# para la parte de red, y que tu usuario normal puede compilar.
if ! command -v erlc >/dev/null 2>&1; then
    echo "[!] No se encontró 'erlc' en tu PATH normal. Instalá Erlang/OTP antes de seguir."
    exit 1
fi
if ! command -v gcc >/dev/null 2>&1; then
    echo "[!] No se encontró 'gcc' en tu PATH normal."
    exit 1
fi

# Resolvemos las rutas ABSOLUTAS de erl/erlc en tu PATH de usuario normal,
# para no depender de qué PATH tenga sudo/root más adelante (ahí está el
# problema típico de Fedora: erlc vive en una ruta que sudo no ve).
ERLC_BIN="$(command -v erlc)"
ERL_BIN="$(command -v erl)"
echo "[*] Usando erlc: $ERLC_BIN"
echo "[*] Usando erl:  $ERL_BIN"

PUERTO=8000
CPU=4
MEM=8192
GPU=1

cleanup() {
    echo "[*] Limpiando..."
    sudo pkill -9 -f ./agente 2>/dev/null
    sudo pkill -9 -f "c_agent_client" 2>/dev/null
    for ns in pc_azul pc_roja pc_verde; do
        sudo ip netns del "$ns" 2>/dev/null
    done
    sudo ip link del br0 2>/dev/null
    stty sane 2>/dev/null
}
trap cleanup EXIT SIGINT SIGTERM
cleanup

echo "[*] Compilando proyecto (agente C + beam de Erlang)..."
# /tmp/build.log puede haber quedado de una corrida vieja con otro dueño
# (ej. si alguna vez se corrió el script con sudo). Lo limpiamos primero
# para no fallar silenciosamente al redirigir la salida de "make" ahí.
sudo rm -f /tmp/build.log
touch /tmp/build.log

make clean >/dev/null 2>&1
make all > /tmp/build.log 2>&1
if [ ! -f ./agente ]; then
    echo "[!] Falló la compilación del agente C. Detalle:"
    cat /tmp/build.log
    exit 1
fi
if [ ! -f ./c_agent_client.beam ]; then
    echo "[!] Falló la compilación del .beam de Erlang. Detalle:"
    cat /tmp/build.log
    exit 1
fi

rm -f log_azul.txt log_roja.txt log_verde.txt planificador.log

echo "[*] Levantando bridge L2 (br0) para que el broadcast UDP llegue a las 3 PCs..."
sudo ip link add br0 type bridge
sudo ip link set br0 up

declare -A IPS=( [pc_azul]=10.0.0.10 [pc_roja]=10.0.0.20 [pc_verde]=10.0.0.30 )

for ns in pc_azul pc_roja pc_verde; do
    ip=${IPS[$ns]}
    veth_ns="veth_${ns#pc_}"
    veth_br="br_${ns#pc_}"

    sudo ip netns add "$ns"
    sudo ip link add "$veth_br" type veth peer name "$veth_ns"
    sudo ip link set "$veth_br" master br0
    sudo ip link set "$veth_br" up
    sudo ip link set "$veth_ns" netns "$ns"

    sudo ip netns exec "$ns" ip addr add "$ip/24" dev "$veth_ns"
    sudo ip netns exec "$ns" ip link set "$veth_ns" up
    sudo ip netns exec "$ns" ip link set lo up
    # Ruta explícita para que sendto(255.255.255.255) salga por la interfaz correcta
    sudo ip netns exec "$ns" ip route add 255.255.255.255 dev "$veth_ns"
done
sleep 1

echo "[*] Levantando agente C en cada PC (con la IP REAL de cada netns, como hacen siempre con hostname -I)..."
sudo ip netns exec pc_roja  stdbuf -oL -eL ./agente 10.0.0.20 $PUERTO $CPU $MEM $GPU < /dev/null > log_roja.txt  2>&1 &
sudo ip netns exec pc_verde stdbuf -oL -eL ./agente 10.0.0.30 $PUERTO $CPU $MEM $GPU < /dev/null > log_verde.txt 2>&1 &
sudo ip netns exec pc_azul  stdbuf -oL -eL ./agente 10.0.0.10 $PUERTO $CPU $MEM $GPU < /dev/null > log_azul.txt  2>&1 &

echo "[*] Arrancando Erlang CASI INMEDIATO (2s) para reproducir el timing real reportado..."
sleep 2

echo "[*] Levantando el planificador Erlang DENTRO de pc_azul (habla con localhost:$PUERTO)..."
sudo ip netns exec pc_azul bash -c "
    cd $(pwd)
    '$ERL_BIN' -pa planificador_erl -noshell -s c_agent_client start
" > erlang_stdout.txt 2>&1 &

ERL_PID=$!
echo "[*] Corriendo. Dejando que el planificador dispare unas ráfagas de jobs (20s)..."
sleep 20

echo "[*] Deteniendo Erlang..."
sudo pkill -9 -f "c_agent_client" 2>/dev/null
sleep 1

stty sane 2>/dev/null
echo ""
echo "======================================================================"
echo " RESULTADOS"
echo "======================================================================"
echo ""
echo "--- Salida del planificador Erlang (erlang_stdout.txt) ---"
cat erlang_stdout.txt
echo ""
echo "--- Log de jobs de Erlang (planificador.log) ---"
cat planificador.log 2>/dev/null || echo "(no se generó planificador.log)"
echo ""
echo "--- Agente C en pc_azul (10.0.0.10) -- ESTE es el que habla con Erlang local ---"
echo "    Buscamos específicamente líneas de ERLANG LOCAL y a qué IP intenta mandar RESERVE:"
grep -E "RX\]|TX\]|JOB_REQUEST|RESERVE|Fallo crítico|connect saliente" log_azul.txt
echo ""
echo "--- Agente C en pc_roja (10.0.0.20) ---"
grep -E "RX\]|TX\]" log_roja.txt
echo ""
echo "--- Agente C en pc_verde (10.0.0.30) ---"
grep -E "RX\]|TX\]" log_verde.txt
echo ""
echo "======================================================================"
echo " DIAGNÓSTICO RÁPIDO"
echo "======================================================================"
echo "Si en log_azul.txt ves algo como:"
echo "  [TX] A AGENTE REMOTO 10.0.0.10 ..."
echo "(es decir, pc_azul mandando un RESERVE/conectándose a SU PROPIA ip 10.0.0.10)"
echo "eso confirma que el JOB_REQUEST de Erlang está usando un host que no matchea"
echo "con el g_ip del agente, y por eso nunca toma la rama 'local' en avanzar_reserva."
echo "======================================================================"