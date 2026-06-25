#!/usr/bin/env python3
"""
Verifica, a partir de los logs de cada agente C (log_pcN.txt), que el
"available" de cada recurso (cpu/mem/gpu) nunca haya quedado negativo ni
por encima del total declarado EN NINGUN MOMENTO de la corrida -- no solo
al final del log, sino en cada paso intermedio. Esto detecta tanto el
sobre-otorgamiento mientras un recurso esta en uso, como una inflacion
temporal que despues se "esconde" porque alguien vuelve a reservar de ahi.

Funciona simulando, linea por linea, los mismos eventos que afectan a
available_amount dentro del agente C real:
  - Recibe un RESERVE de un agente remoto y responde GRANTED -> resta.
  - Su propio Erlang local manda JOB_RELEASE <job_id> -> suma TODO lo que
    ese job tenia otorgado segun nuestro propio registro (no segun el
    mensaje).
  - Un agente remoto manda RELEASE <job> <recurso> <amount> -> sumamos lo
    que NOSOTROS tenemos anotado para ese job/recurso, ignorando el
    "amount" del mensaje (asi se valida que el fix de no confiar en el
    mensaje siga funcionando).

Despues de cada uno de estos eventos se chequea si available quedo fuera
del rango [0, total] para ese recurso puntual, y se reporta la linea
exacta donde ocurrio cada violacion.

Uso:
    python3 track_resources.py <carpeta_logs> <pc1>:<cpu>:<mem>:<gpu> [<pc2>:... ...]

Ejemplo:
    python3 track_resources.py logs pc1:3:4096:1 pc2:4:8192:0 pc3:5:12288:1
"""

import re
import sys


def parse_totales(args):
    totales = {}
    for arg in args:
        partes = arg.split(":")
        if len(partes) != 4:
            print(f"[!] Formato invalido para '{arg}', se espera pcN:cpu:mem:gpu")
            sys.exit(1)
        nombre, cpu, mem, gpu = partes
        totales[nombre] = {"cpu": int(cpu), "mem": int(mem), "gpu": int(gpu)}
    return totales


def trackear(carpeta_logs, pc, totales):
    fname = f"{carpeta_logs}/log_{pc}.txt"
    available = dict(totales)
    granted_per_job = {}  # job_id -> {recurso: amount}
    pending_reserve = None
    violations = []  # (lineno, line, tipo, recurso, valor)

    def chequear(lineno, line, recurso):
        """Registra una violacion si available quedo fuera del rango [0, total]."""
        valor = available[recurso]
        if valor < 0:
            violations.append((lineno, line, "NEGATIVO", recurso, valor))
        elif valor > totales[recurso]:
            violations.append((lineno, line, "SUPERA EL TOTAL", recurso, valor))

    try:
        with open(fname) as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"[!] No se encontro {fname}, saltando.")
        return

    for lineno, line in enumerate(lines, 1):
        line = line.strip()

        m = re.search(r'RX\] De AGENTE REMOTO.*-> RESERVE (\d+) (cpu|mem|gpu) (\d+)', line)
        if m:
            pending_reserve = (m.group(1), m.group(2), int(m.group(3)))
            continue

        m = re.search(r'TX\] A AGENTE REMOTO.*-> GRANTED (\d+)', line)
        if m and pending_reserve and pending_reserve[0] == m.group(1):
            job_id, recurso, amount = pending_reserve
            available[recurso] -= amount
            granted_per_job.setdefault(job_id, {}).setdefault(recurso, 0)
            granted_per_job[job_id][recurso] += amount
            chequear(lineno, line, recurso)
            pending_reserve = None
            continue

        m = re.search(r'RX\] De ERLANG LOCAL.*-> JOB_RELEASE (\d+)', line)
        if m:
            job_id = m.group(1)
            if job_id in granted_per_job:
                for recurso, amount in granted_per_job[job_id].items():
                    available[recurso] += amount
                    chequear(lineno, line, recurso)
                del granted_per_job[job_id]
            continue

        m = re.search(r'RX\] De AGENTE REMOTO.*-> RELEASE (\d+) (cpu|mem|gpu) (\d+)', line)
        if m:
            job_id, recurso = m.group(1), m.group(2)
            if job_id in granted_per_job and recurso in granted_per_job[job_id]:
                real_amount = granted_per_job[job_id][recurso]
                available[recurso] += real_amount
                chequear(lineno, line, recurso)
                del granted_per_job[job_id][recurso]
            continue

    print(f"=== {pc} (totales: {totales}) ===")
    print(f"  available final: {available}")
    if violations:
        print(f"  !!! {len(violations)} VIOLACIONES detectadas:")
        for v in violations[:10]:
            lineno, line, tipo, recurso, valor = v
            print(f"      linea {lineno} [{tipo}] {recurso}={valor}: {line}")
        if len(violations) > 10:
            print(f"      ... y {len(violations) - 10} mas")
    else:
        print("  Sin violaciones (available nunca fue negativo ni superó el total)")
    print()


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    carpeta_logs = sys.argv[1]
    totales = parse_totales(sys.argv[2:])

    for pc in totales:
        trackear(carpeta_logs, pc, totales[pc])


if __name__ == "__main__":
    main()