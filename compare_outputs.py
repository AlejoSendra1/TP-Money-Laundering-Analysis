"""
compare_outputs.py
==================
Compara los archivos de resultados CSV entre dos ejecuciones del sistema
(ej: secuencial vs. concurrente con múltiples pods).

El script parsea el docker-compose YAML para descubrir automáticamente
los clientes y los archivos de resultados que genera cada uno.

Cómo funciona el naming en el cliente Go
-----------------------------------------
  OUTPUT_FILE=/output/output_0.csv
  → basePath  = filepath.Dir("/output/output_0.csv") = "/output"
  → por query = filepath.Join(basePath, basePath + "_results_qN.csv")
              = "/output/output_results_qN.csv"
  → en el host (volume ./output_seq:/output) = ./output_seq/output_results_qN.csv

Lo que cambia entre ejecuciones es sólo el directorio host del volumen,
p. ej.:
  Secuencial  → ./output_seq:/output
  Concurrente → ./output_conc:/output

Uso
---
  # Comparar usando el YAML para saber los clientes/archivos
  python3 compare_outputs.py --yaml scenarios/full.yaml \\
                              ./output_seq ./output_conc

  # Comparar sólo queries específicas
  python3 compare_outputs.py --yaml scenarios/full.yaml \\
                              ./output_seq ./output_conc --queries q1 q4

  # Sin YAML: auto-descubrir archivos *_results_q*.csv en los directorios
  python3 compare_outputs.py ./output_seq ./output_conc
"""

import csv
import sys
import re
import argparse
from collections import Counter
from pathlib import Path, PurePosixPath

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

# Queries conocidas y sus nombres de archivo tal como los genera el Go
KNOWN_QUERIES = ["q1", "q2", "q3", "q4", "q5"]

# Patrón que generan los clientes Go:
#   fmt.Sprintf("%s_results_%s.csv", basePath, queryName)
#   con basePath = filepath.Dir(OUTPUT_FILE) dentro del contenedor
# Como basePath siempre es "/output", el archivo del contenedor queda:
#   /output/output_results_qN.csv  →  host: <output_dir>/output_results_qN.csv
CONTAINER_OUTPUT_DIR = "/output"


# ---------------------------------------------------------------------------
# Parsing del YAML para descubrir clientes y sus archivos
# ---------------------------------------------------------------------------

def parse_yaml_clients(yaml_path: Path) -> list[dict]:
    """
    Lee un docker-compose YAML y devuelve la lista de clientes con:
      {
        "name":        str,   # container_name
        "output_file": str,   # valor de OUTPUT_FILE (path dentro del contenedor)
        "output_dir":  str,   # directorio contenedor del output (filepath.Dir)
        "result_files": list[str],  # nombres de archivo en el host (sin directorio)
      }
    """
    if not HAS_YAML:
        print("⚠  PyYAML no instalado. Instálalo con: pip install pyyaml", file=sys.stderr)
        return []

    with open(yaml_path, "r") as f:
        compose = yaml.safe_load(f)

    clients = []
    services = compose.get("services", {})

    for svc_name, svc in services.items():
        if not svc_name.startswith("client"):
            continue

        env = svc.get("environment", [])
        output_file = _find_env(env, "OUTPUT_FILE")
        if not output_file:
            continue

        # Replicar lógica Go: basePath = filepath.Dir(OUTPUT_FILE)
        container_output_dir = str(PurePosixPath(output_file).parent)  # "/output"

        # Nombre base del directorio  (p. ej. "output" de "/output")
        dir_stem = PurePosixPath(container_output_dir).name  # "output"

        # Archivos resultado: fmt.Sprintf("%s_results_%s.csv", basePath, queryName)
        # → "/output_results_qN.csv"  → filepath.Join("/output", ...) → "/output/output_results_qN.csv"
        # El nombre de archivo en el host (dentro del dir mapeado):
        result_files = [
            f"{dir_stem}_results_{q}.csv"
            for q in KNOWN_QUERIES
        ]

        clients.append({
            "name": svc.get("container_name", svc_name),
            "output_file": output_file,
            "output_dir": container_output_dir,
            "result_files": result_files,
        })

    return clients


def _find_env(env_list: list, key: str) -> str | None:
    """Busca una variable de entorno en la lista `- KEY=VALUE`."""
    for entry in env_list:
        if isinstance(entry, str) and entry.startswith(key + "="):
            return entry.split("=", 1)[1]
        if isinstance(entry, dict) and key in entry:
            return str(entry[key])
    return None


# ---------------------------------------------------------------------------
# Función principal de comparación de dos archivos CSV
# ---------------------------------------------------------------------------

def compare_csv(file_a: str | Path, file_b: str | Path) -> dict:
    """
    Compara dos archivos CSV sin importar el orden de las filas.

    Usa Counter (multiconjunto) para detectar diferencias incluso con duplicados.

    Devuelve:
        {
            "equal":        bool,
            "rows_a":       int,
            "rows_b":       int,
            "only_in_a":    list[tuple],   # filas en A pero no en B
            "only_in_b":    list[tuple],   # filas en B pero no en A
            "header_a":     list[str],
            "header_b":     list[str],
            "header_match": bool,
        }
    """
    rows_a, header_a = _read_csv(Path(file_a))
    rows_b, header_b = _read_csv(Path(file_b))

    header_match = header_a == header_b

    counter_a = Counter(rows_a)
    counter_b = Counter(rows_b)

    only_in_a = list((counter_a - counter_b).elements())
    only_in_b = list((counter_b - counter_a).elements())

    equal = header_match and not only_in_a and not only_in_b

    return {
        "equal":        equal,
        "rows_a":       len(rows_a),
        "rows_b":       len(rows_b),
        "only_in_a":    only_in_a,
        "only_in_b":    only_in_b,
        "header_a":     header_a,
        "header_b":     header_b,
        "header_match": header_match,
    }


def _read_csv(path: Path) -> tuple[list[tuple], list[str]]:
    """Lee un CSV y devuelve (filas_como_tuplas, cabecera)."""
    rows, header = [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return rows, header
        for row in reader:
            if any(cell.strip() for cell in row):
                rows.append(tuple(cell.strip() for cell in row))
    return rows, header


# ---------------------------------------------------------------------------
# Descubrimiento de archivos CSV en un directorio
# ---------------------------------------------------------------------------

def discover_csv_files(directory: Path) -> list[str]:
    """Devuelve los nombres de todos los archivos .csv del directorio (sin subdirectorios)."""
    return sorted(p.name for p in directory.glob("*.csv"))


# ---------------------------------------------------------------------------
# Lógica principal de comparación entre dos ejecuciones
# ---------------------------------------------------------------------------

def compare_runs(
    dir_a:      Path,
    dir_b:      Path,
    clients:    list[dict] | None = None,
    queries:    list[str]  | None = None,
    max_diffs:  int = 10,
    label_a:    str = "Ejecución A",
    label_b:    str = "Ejecución B",
) -> bool:
    """
    Compara los archivos CSV entre dos directorios de output.

    Estrategia de selección de archivos (en orden de prioridad):
      1. Si se provee --yaml: usa los archivos esperados por cliente.
      2. Si no: compara TODOS los .csv de ambos directorios por nombre homónimo.

    En ambos casos se puede filtrar por --queries qN.

    Retorna True si todos los archivos comparados son iguales.
    """
    print()
    print("=" * 65)
    print(f"  Comparando ejecuciones")
    print(f"  A ({label_a}): {dir_a.resolve()}")
    print(f"  B ({label_b}): {dir_b.resolve()}")
    print("=" * 65)

    all_equal = True

    if clients:
        # --- Modo YAML: iterar por cliente → por query ---
        for client in clients:
            print(f"\n  Cliente: {client['name']}  (OUTPUT_FILE={client['output_file']})")
            for filename in client["result_files"]:
                query = _query_from_filename(filename)
                if queries and query not in queries:
                    continue
                ok = _compare_and_print(filename, dir_a / filename, dir_b / filename, max_diffs, query)
                if not ok:
                    all_equal = False

    else:
        # --- Modo auto: todos los .csv de ambos directorios ---
        files_a = set(discover_csv_files(dir_a))
        files_b = set(discover_csv_files(dir_b))
        all_files = sorted(files_a | files_b)

        if not all_files:
            print("\n  ⚠  No se encontraron archivos .csv en ninguno de los directorios.")
            return False

        # Avisar archivos que sólo están en uno de los dos lados
        only_a = files_a - files_b
        only_b = files_b - files_a
        if only_a:
            print(f"\n  ⚠  Archivos sólo en A (no comparados): {sorted(only_a)}")
        if only_b:
            print(f"\n  ⚠  Archivos sólo en B (no comparados): {sorted(only_b)}")

        for filename in sorted(files_a & files_b):  # sólo los que existen en ambos
            query = _query_from_filename(filename)
            if queries and query not in queries:
                continue
            ok = _compare_and_print(filename, dir_a / filename, dir_b / filename, max_diffs, query)
            if not ok:
                all_equal = False

        if only_a or only_b:
            all_equal = False  # archivos sin par también cuentan como diferencia

    print()
    print("=" * 65)
    if all_equal:
        print("  ✅  Todos los outputs son IGUALES entre las dos ejecuciones.")
    else:
        print("  ❌  Se encontraron DIFERENCIAS entre las ejecuciones.")
    print("=" * 65)
    print()

    return all_equal


def _query_from_filename(filename: str) -> str | None:
    """Extrae 'q1'..'q5' del nombre de un archivo resultado."""
    m = re.search(r"_results_(q\d+)\.csv$", filename)
    return m.group(1) if m else None


def _compare_and_print(filename: str, file_a: Path, file_b: Path, max_diffs: int, query: str | None) -> bool:
    label = query.upper() if query else filename
    print(f"\n  --- {label} ({filename}) ---")

    if not file_a.exists():
        print(f"  ⚠  Archivo no encontrado en A: {file_a}")
        return False
    if not file_b.exists():
        print(f"  ⚠  Archivo no encontrado en B: {file_b}")
        return False

    result = compare_csv(file_a, file_b)

    print(f"  Filas  A / B : {result['rows_a']} / {result['rows_b']}")

    if not result["header_match"]:
        print(f"  ⚠  Cabeceras distintas!")
        print(f"     A: {result['header_a']}")
        print(f"     B: {result['header_b']}")

    if result["equal"]:
        print("  ✅  IGUALES")
    else:
        print("  ❌  DIFERENCIAS:")
        _print_diffs("sólo en A", result["only_in_a"], max_diffs)
        _print_diffs("sólo en B", result["only_in_b"], max_diffs)

    return result["equal"]


def _print_diffs(label: str, rows: list[tuple], max_diffs: int):
    if not rows:
        return
    print(f"    Filas {label} ({len(rows)} registros):")
    for row in rows[:max_diffs]:
        print(f"      {list(row)}")
    if len(rows) > max_diffs:
        print(f"      ... y {len(rows) - max_diffs} más")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compara los CSVs de output entre dos ejecuciones del sistema "
            "(secuencial vs. concurrente). El orden de las filas no importa."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Con YAML (recomendado): descubre clientes y archivos automáticamente
  python3 compare_outputs.py --yaml scenarios/full.yaml ./output_seq ./output_conc

  # Sin YAML: busca todos los *_results_q*.csv en los directorios
  python3 compare_outputs.py ./output_seq ./output_conc

  # Sólo comparar queries específicas
  python3 compare_outputs.py --yaml scenarios/full.yaml ./output_seq ./output_conc --queries q1 q4

  # Etiquetas personalizadas para el reporte
  python3 compare_outputs.py ./output_seq ./output_conc --label-a "Secuencial" --label-b "4 pods"
        """
    )
    parser.add_argument("dir_a", help="Directorio de output de la ejecución A (ej: ./output_seq)")
    parser.add_argument("dir_b", help="Directorio de output de la ejecución B (ej: ./output_conc)")
    parser.add_argument("--yaml", metavar="COMPOSE_YAML",
                        help="Path al docker-compose YAML para descubrir clientes y archivos")
    parser.add_argument("--queries", nargs="+", metavar="qN",
                        choices=KNOWN_QUERIES, default=None,
                        help="Queries a comparar (default: todas). Ej: --queries q1 q4")
    parser.add_argument("--label-a", default="Ejecución A",
                        help="Etiqueta descriptiva para el directorio A")
    parser.add_argument("--label-b", default="Ejecución B",
                        help="Etiqueta descriptiva para el directorio B")
    parser.add_argument("--max-diffs", type=int, default=10,
                        help="Máximo de diferencias a mostrar por archivo (default: 10)")

    args = parser.parse_args()

    dir_a = Path(args.dir_a)
    dir_b = Path(args.dir_b)

    for label, d in [("dir_a", dir_a), ("dir_b", dir_b)]:
        if not d.is_dir():
            print(f"Error: '{d}' no es un directorio válido ({label}).", file=sys.stderr)
            sys.exit(1)

    clients = None
    if args.yaml:
        yaml_path = Path(args.yaml)
        if not yaml_path.exists():
            print(f"Error: YAML no encontrado: {yaml_path}", file=sys.stderr)
            sys.exit(1)
        clients = parse_yaml_clients(yaml_path)
        if not clients:
            print(f"⚠  No se encontraron clientes en {yaml_path}. Usando auto-descubrimiento.")

    ok = compare_runs(
        dir_a=dir_a,
        dir_b=dir_b,
        clients=clients,
        queries=args.queries,
        max_diffs=args.max_diffs,
        label_a=args.label_a,
        label_b=args.label_b,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()


