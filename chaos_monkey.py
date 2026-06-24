import subprocess
import random
import time
from datetime import datetime
import sys
import yaml

def get_timestamp():
    return datetime.now().strftime("[%H:%M:%S]")

def load_config(config_path="chaos_config.yaml"):
    """Carga la configuracion desde el archivo YAML"""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            return config.get("chaos_monkey", {})
    except FileNotFoundError:
        print(f"{get_timestamp()} Error: No se encontró el archivo '{config_path}'.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"{get_timestamp()} Error al leer el YAML: {e}")
        sys.exit(1)

def get_running_workers(excluded_containers, target_containers):    
    """
    Obtiene los contenedores corriendo
    1. Descarta los excluidos
    2. Si 'target_containers' tiene elementos, devuelve solo los que coincidan
       Si esta vacío, devuelve todos los que quedaron
    """    
    try:
        result = subprocess.run(
            ['docker', 'ps', '--format', '{{.Names}}'], 
            capture_output=True, text=True, check=True
        )
        all_containers = result.stdout.strip().split('\n')
        
        valid_workers = []
        for container in all_containers:
            if not container:
                continue
            
            # Filtramos los excluidos
            is_excluded = any(excl_name in container for excl_name in excluded_containers)
            if is_excluded:
                continue
                
            # Si la lista de targets no está vacía, chequeamos si el contenedor es un target
            if target_containers:
                is_target = any(target_name in container for target_name in target_containers)
                if is_target:
                    valid_workers.append(container)
            else:
                # Si no hay targets definidos, todos los que no están excluidos son víctimas
                valid_workers.append(container)
                
        return valid_workers
    except Exception as e:
        print(f"{get_timestamp()} Error al comunicarse con Docker: {e}")
        return []

def kill_container(container_name):
    """Fuerza el apagado inmediato del contenedor (SIGKILL)."""
    subprocess.run(['docker', 'kill', container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{get_timestamp()} KILL: El nodo '{container_name}' ha sido asesinado")

def main():
    print("Iniciando Chaos Monkey...")
    print("Cargando configuracion desde 'chaos_config.yaml'...")
    
    config = load_config()
    attack_interval = config.get("attack_interval", 20)
    excluded_containers = config.get("excluded_containers", ["rabbitmq", "gateway", "client"])
    target_containers = config.get("target_containers", [])
    if target_containers is None:
        target_containers = []
    if target_containers:
        print(f"Atacando SOLO a: {target_containers}")
    else:
        print(f"Atacando a cualquier contenedor disponible")
    print(f"Intervalo de ataque: {attack_interval}s")
    print(f"Ignorando contenedores {excluded_containers}")
    print("Presiona Ctrl+C para detener la ejecución.\n")
    
    try:
        while True:
            workers = get_running_workers(excluded_containers, target_containers)
            
            if not workers:
                # Caso muy extremo
                print(f"{get_timestamp()} No se encontraron nodos activos para atacar. Reintentando en 5s...") 
                time.sleep(5)
                continue
                
            victim = random.choice(workers)
            
            kill_container(victim)
            
            print(f"{get_timestamp()} Esperando {attack_interval} segundos...\n")
            time.sleep(attack_interval)
            
    except KeyboardInterrupt:
        print("\nChaos Monkey detenido por el usuario")
        sys.exit(0)

if __name__ == "__main__":
    main()