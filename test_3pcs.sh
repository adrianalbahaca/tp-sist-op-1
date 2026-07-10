#!/bin/bash
# =========================================================================
# Simulación de N "PCs" en una LAN virtual (netns + bridge), cada una con
# su propio agente C Y su propio planificador Erlang -- como en el
# laboratorio real, donde cada compañero corre ambos en su máquina.
#
# Uso:
#   ./test_3pcs.sh N
#
# Ejemplos:
#   ./test_3pcs.sh 3      -> simula 3 PCs (pc1, pc2, pc3)
#   ./test_3pcs.sh 6      -> simula 6 PCs (pc1 .. pc6)
#
# Topología:
#   br0 (bridge, sin IP, solo para que el broadcast UDP llegue a todas)
#    ├── pc1  10.0.0.1   -> agente C + planificador Erlang
#    ├── pc2  10.0.0.2   -> agente C + planificador Erlang
#    ├── ...
#    └── pcN  10.0.0.N   -> agente C + planificador Erlang
#
# Usalo desde la raíz del repo (TP-Final/), donde está el Makefile.
#
# IMPORTANTE: NO correr este script con "sudo bash ./test_3pcs.sh".
# Corrarlo como tu usuario normal: ./test_3pcs.sh N
# El script pide sudo SOLO para los comandos de red (ip netns / ip link).
# Si se corre todo bajo sudo, "make all" hereda el PATH restringido de sudo
# (secure_path) y puede no encontrar erlc/erl aunque "which erlc" sí los vea
# en tu shell normal. Por eso separamos: compilación sin sudo, red con sudo.
# =========================================================================

set -u

# -------------------------------------------------------------------------
# Cantidad de PCs a simular: viene como primer argumento del script.
# -------------------------------------------------------------------------
if [ $# -ne 1 ]; then
    echo "Uso: $0 <cantidad_de_pcs>"
    echo "Ejemplo: $0 5"
    exit 1
fi

N_PCS=$1

if ! [[ "$N_PCS" =~ ^[0-9]+$ ]] || [ "$N_PCS" -lt 1 ]; then
    echo "[!] La cantidad de PCs debe ser un número entero mayor a 0."
    exit 1
fi

if [ "$N_PCS" -gt 253 ]; then
    echo "[!] Con la topología actual (10.0.0.X/24) el máximo razonable es 253 PCs."
    exit 1
fi

if [ "$EUID" -eq 0 ]; then
    echo "[!] No corras este script con sudo/como root directamente."
    echo "    Corrélo como tu usuario normal: ./test_3pcs.sh $N_PCS"
    echo "    (el script va a pedir sudo solo donde lo necesita, para 'ip netns'/'ip link')"
    exit 1
fi

# Verificación temprana: confirmar que tu usuario normal puede compilar.
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
echo "[*] Simulando $N_PCS PC(s)."

declare -A IPS
declare -A CPUS
declare -A MEMS
declare -A GPUS
PC_NAMES=()
for i in $(seq 1 "$N_PCS"); do
    nombre="pc${i}"
    PC_NAMES+=("$nombre")
    IPS[$nombre]="10.0.0.${i}"
    CPUS[$nombre]=$((2 * i))          # pc1->3, pc2->4, pc3->5, ...
    MEMS[$nombre]=$((8192 * i))       # pc1->4096, pc2->8192, pc3->12288, ...
    GPUS[$nombre]=$((i % 2))          # alterna 1, 0, 1, 0, ...
done

declare PUERTO=8000
# -------------------------------------------------------------------------
# Generamos la lista de nombres de PC (pc1, pc2, ..., pcN) y su IP
# correspondiente (10.0.0.1, 10.0.0.2, ..., 10.0.0.N) en un array asociativo,
# en vez de tenerlos escritos a mano como antes.
# -------------------------------------------------------------------------

cleanup() {
    echo "[*] Limpiando..."
    sudo pkill -9 -f ./agente 2>/dev/null
    sudo pkill -9 -f "c_agent_client" 2>/dev/null
    for ns in "${PC_NAMES[@]}"; do
        sudo ip netns del "$ns" 2>/dev/null
    done
    sudo ip link del br0 2>/dev/null
    stty sane 2>/dev/null
}
trap cleanup EXIT SIGINT SIGTERM
cleanup

mkdir -p logs

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

rm -f logs/log_*.txt logs/erlang_*.txt planificador.log

echo "[*] Levantando bridge L2 (br0) para que el broadcast UDP llegue a todas las PCs..."
sudo ip link add br0 type bridge
sudo ip link set br0 up

for ns in "${PC_NAMES[@]}"; do
    ip=${IPS[$ns]}
    veth_ns="veth_${ns}"
    veth_br="br_${ns}"

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

echo "[*] Levantando agente C en cada una de las $N_PCS PC(s) (con la IP REAL de cada netns)..."
for ns in "${PC_NAMES[@]}"; do
    ip=${IPS[$ns]}
    cpu=${CPUS[$ns]}
    mem=${MEMS[$ns]}
    gpu=${GPUS[$ns]}
    sudo ip netns exec "$ns" stdbuf -oL -eL ./agente "$ip" $PUERTO "$cpu" "$mem" "$gpu" < /dev/null > "logs/log_${ns}.txt" 2>&1 &
done

echo "[*] Arrancando Erlang CASI INMEDIATO (2s) para reproducir el timing real reportado..."
sleep 2

echo "[*] Levantando el planificador Erlang DENTRO de cada PC (cada una habla con su propio localhost:$PUERTO)..."
for ns in "${PC_NAMES[@]}"; do
    sudo ip netns exec "$ns" bash -c "
        cd $(pwd)
        '$ERL_BIN' -pa planificador_erl -noshell -s c_agent_client start
    " > "logs/erlang_${ns}.txt" 2>&1 &
done

echo "[*] Corriendo. Dejando que los planificadores disparen unas ráfagas de jobs (60s)..."
sleep 120

echo "[*] Deteniendo todas las instancias de Erlang..."
sudo pkill -9 -f "c_agent_client" 2>/dev/null
sleep 1

stty sane 2>/dev/null
echo ""
echo "======================================================================"
echo " RESULTADOS"
echo "======================================================================"

for ns in "${PC_NAMES[@]}"; do
    ip=${IPS[$ns]}
    echo ""
    echo "--- $ns ($ip) -- salida de Erlang (logs/erlang_${ns}.txt) ---"
    cat "logs/erlang_${ns}.txt" 2>/dev/null || echo "(no se generó el log)"
done

echo ""
echo "--- Log de jobs combinado de todas las PCs (planificador.log) ---"
cat planificador.log 2>/dev/null || echo "(no se generó planificador.log)"

for ns in "${PC_NAMES[@]}"; do
    ip=${IPS[$ns]}
    echo ""
    echo "--- Agente C en $ns ($ip) ---"
    echo "    Buscamos líneas de ERLANG LOCAL y a qué IP intenta mandar RESERVE:"
    grep -E "RX\]|TX\]|JOB_REQUEST|RESERVE|Fallo crítico|connect saliente|mal formado" "logs/log_${ns}.txt" 2>/dev/null
done
