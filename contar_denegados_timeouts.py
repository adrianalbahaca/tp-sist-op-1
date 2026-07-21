#!/usr/bin/env python3
"""
Cuenta ocurrencias de "DENEGADO" y "timeout" (sin distinguir mayúsculas/minúsculas)
en uno o más archivos de log de Erlang.

Uso:
    python3 contar_denegados_timeouts.py logs/erlang_pc1.txt logs/erlang_pc2.txt ...
    python3 contar_denegados_timeouts.py logs/erlang_*.txt
"""

import sys
import re
import glob


def contar_en_archivo(path):
    denegados = 0
    timeouts = 0
    total_lineas = 0

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for linea in f:
            total_lineas += 1
            if re.search(r"denegado", linea, re.IGNORECASE):
                denegados += 1
            if re.search(r"timeout", linea, re.IGNORECASE):
                timeouts += 1

    return denegados, timeouts, total_lineas


def main():
    if len(sys.argv) < 2:
        print("Uso: python3 contar_denegados_timeouts.py <archivo1> [archivo2] ...")
        print("     (soporta wildcards, ej: logs/erlang_*.txt)")
        sys.exit(1)

    # Expandir wildcards por si el shell no lo hizo (ej. en algunos entornos Windows)
    archivos = []
    for patron in sys.argv[1:]:
        expandido = glob.glob(patron)
        archivos.extend(expandido if expandido else [patron])

    if not archivos:
        print("No se encontraron archivos que coincidan con los patrones dados.")
        sys.exit(1)

    total_denegados = 0
    total_timeouts = 0
    total_lineas_global = 0

    print(f"{'Archivo':<35} {'DENEGADO':>10} {'TIMEOUT':>10} {'Líneas':>10}")
    print("-" * 68)

    for archivo in sorted(archivos):
        try:
            denegados, timeouts, lineas = contar_en_archivo(archivo)
        except FileNotFoundError:
            print(f"{archivo:<35} {'(no encontrado)':>32}")
            continue
        except Exception as e:
            print(f"{archivo:<35} (error: {e})")
            continue

        total_denegados += denegados
        total_timeouts += timeouts
        total_lineas_global += lineas

        print(f"{archivo:<35} {denegados:>10} {timeouts:>10} {lineas:>10}")

    print("-" * 68)
    print(f"{'TOTAL':<35} {total_denegados:>10} {total_timeouts:>10} {total_lineas_global:>10}")

    if total_denegados + total_timeouts > 0 and total_lineas_global > 0:
        print(f"\nTasa de DENEGADO: {100 * total_denegados / total_lineas_global:.2f}% de las líneas")
        print(f"Tasa de TIMEOUT:  {100 * total_timeouts / total_lineas_global:.2f}% de las líneas")


if __name__ == "__main__":
    main()
