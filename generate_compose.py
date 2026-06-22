import yaml
import sys

def main():
    # ==============================================================================
    # 0. VALIDACION ARGUMENTOS
    # ==============================================================================
    if len(sys.argv) < 2:
        print("Error: Se requiere especificar el nombre del archivo de salida como argumento.")
        print("Uso sugerido: python generate_compose.py <nombre_archivo.yaml>")
        sys.exit(1)

    output_filename = sys.argv[1]

    # ==============================================================================
    # 1. CONFIGURACION
    # ==============================================================================
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            SCALE = config.get("scale", {})
            CLIENTS = config.get("clients", [])
            WATCHDOG_AMOUNT = config.get("watchdog_amount", 3)
    except FileNotFoundError:
        print("Error: No se encontró 'config.yaml'.")
        sys.exit(1)

    services = {}

    # ==============================================================================
    # 2. HELPERS
    # ==============================================================================
    def get_deps(downstream_nodes):
        """Calcula de que contenedores depende un nodo según la escala"""
        deps = {"rabbitmq": {"condition": "service_healthy"}}
        for node in downstream_nodes:
            if node == "gateway":
                deps["gateway"] = {"condition": "service_started"}
            elif node in SCALE:
                for i in range(SCALE[node]):
                    deps[f"{node}_{i}"] = {"condition": "service_started"}
        return deps

    def add_worker(node_name, downstream_nodes, env_vars, volumes=["./persistence:/persistence"]):
        """Genera dinamicamente las replicas para un tipo de worker"""
        amount = SCALE.get(node_name, 0)
        for i in range(amount):
            env = [f"ID={i}", "MOM_HOST=rabbitmq", "MOM_PORT=5672", f"WORKER_ID={node_name}_{i}"] + env_vars
            service = {
                "build": {"context": "./src/", "dockerfile": f"{node_name}/Dockerfile"},
                "container_name": f"{node_name}_{i}",
                "depends_on": get_deps(downstream_nodes),
                "environment": env
            }
            if volumes:
                service["volumes"] = list(volumes)
            services[f"{node_name}_{i}"] = service

    # ==============================================================================
    # 3. COMPONENTES BASE
    # ==============================================================================
    services["rabbitmq"] = {
        "build": {"context": "./src/rabbitmq", "dockerfile": "Dockerfile"},
        "container_name": "rabbitmq",
        "environment": ["RABBITMQ_LOG_LEVELS=error"],
        "logging": {"driver": "none"},
        "healthcheck": {
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "interval": "5s", 
            "timeout": "3s", "retries": 10, "start_period": "50s"
        },
        "ports": ["5672:5672", "15672:15672"]
    }

    services["gateway"] = {
        "build": {"context": "./src/", "dockerfile": "gateway/Dockerfile"},
        "container_name": "gateway",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": [
            "INPUT_QUEUE=results_queue", "MOM_HOST=rabbitmq", "MOM_PORT=5672",
            "OUTPUT_EXCHANGE_NAME=transactions_exchange", "OUTPUT_TOPIC=transactions_topic", 
            "SERVER_HOST=gateway", "SERVER_PORT=5678",
            f"EOF_EXPECTED_BY_QUERY_1={SCALE.get('q1_amount_filter', 1)}",
            f"EOF_EXPECTED_BY_QUERY_2={SCALE.get('join_q2', 1)}",
            f"EOF_EXPECTED_BY_QUERY_3={SCALE.get('q3_amount_filter', 1)}",
            f"EOF_EXPECTED_BY_QUERY_4={SCALE.get('q4_join', 1)}",
            f"EOF_EXPECTED_BY_QUERY_5={SCALE.get('counter_q5', 1)}"
        ]
    }

    # ==============================================================================
    # 4. CLIENTES
    # ==============================================================================
    if not CLIENTS:
        CLIENTS = [{"batch_size": 200, "input_file": "/datasets/input_0.csv"}]

    for idx, client_cfg in enumerate(CLIENTS):
        client_name = f"client_{idx}"
        batch_size = client_cfg.get("batch_size", 200)
        input_file = client_cfg.get("input_file", f"/datasets/input_{idx}.csv")
        
        services[client_name] = {
            "build": {"context": "./src/", "dockerfile": "client/Dockerfile"},
            "container_name": client_name,
            "depends_on": get_deps(["gateway", "usd_filter", "q5_date_filter"]),
            "environment": [
                f"ID={idx}"
                f"BATCH_SIZE={batch_size}", 
                f"INPUT_FILE={input_file}",
                f"OUTPUT_FILE=/output/output_{client_name}.csv",
                f"WORKER_ID={client_name}",
                "SERVER_HOST=gateway",
                "SERVER_PORT=5678"
            ],
            "volumes": ["./datasets:/datasets", "./output:/output", "./persistence:/persistence"]
        }

    # ==============================================================================
    # 5. COMPONENTES DEL FLUJO DE PROCESAMIENTO
    # ==============================================================================
    
    # FLUJO COMÚN
    add_worker("usd_filter", ["q1_amount_filter", "counter_q2", "date_filter"], [
        "INPUT_QUEUE=usd_filter_queue", "INPUT_EXCHANGE_NAME=transactions_exchange",
        "INPUT_TOPIC=transactions_topic", "OUTPUT_EXCHANGE_NAME=usd_exchange",
        "OUTPUT_TOPIC=usd_transactions_topic", "CONTROL_EXCHANGE_NAME=usd_filter_control_exchange",
        "CONTROL_TOPIC=usd_filter_control_topic"
    ])
    add_worker("date_filter", ["sum", "group", "transactions_saver"], [
        "INPUT_QUEUE=date_filter_queue", "INPUT_EXCHANGE_NAME=usd_exchange",
        "INPUT_TOPIC=usd_transactions_topic", "OUTPUT_EXCHANGE_NAME=date_exchange",
        "OUTPUT_TOPIC_1=usd_early_period_transactions_topic", "OUTPUT_TOPIC_2=usd_later_period_transactions_topic",
        "CONTROL_EXCHANGE_NAME=date_filter_control_exchange", "CONTROL_TOPIC=date_filter_control_topic",
        f"USD_FILTER_AMOUNT={SCALE.get('usd_filter', 1)}"
    ])

    # QUERY 1
    add_worker("q1_amount_filter", [], [
        "INPUT_QUEUE=amount_filter_q1_queue", "INPUT_EXCHANGE_NAME=usd_exchange",
        "INPUT_TOPIC=usd_transactions_topic", "OUTPUT_QUEUE=results_queue",
        "CONTROL_EXCHANGE_NAME=q1_amount_filter_control_exchange", "CONTROL_TOPIC=q1_amount_filter_control_topic",
        f"USD_FILTER_AMOUNT={SCALE.get('usd_filter', 1)}"
    ])

    # QUERY 2
    add_worker("counter_q2", ["join_q2"], [
        "INPUT_QUEUE=counter_q2_queue", "INPUT_EXCHANGE_NAME=usd_exchange",
        "INPUT_TOPIC=usd_transactions_topic", "OUTPUT_PREFIX=join_q2",
        f"JOIN_AMOUNT={SCALE.get('join_q2', 1)}", f"COUNTER_AMOUNT={SCALE.get('counter_q2', 1)}",
        "COUNTER_Q2_CONTROL=counter_q2_control", "BATCH_SIZE=100", f"USD_FILTER_AMOUNT={SCALE.get('usd_filter', 1)}"
    ])
    add_worker("join_q2", [], [
        "INPUT_PREFIX=join_q2", f"COUNTER_AMOUNT={SCALE.get('counter_q2', 1)}",
        "OUTPUT_QUEUE=results_queue", "QUERY_ID=5", "BATCH_SIZE=100"
    ])

    # QUERY 3
    add_worker("sum", ["promediator"], [
        "INPUT_QUEUE=sum_queue", "INPUT_TOPIC=usd_early_period_transactions_topic",
        "INPUT_EXCHANGE_NAME=date_exchange", "CONTROL_EXCHANGE_NAME=sum_control_exchange",
        "CONTROL_EXCHANGE_TOPIC=sum_control_topic", "OUTPUT_EXCHANGE_NAME=sum_results_exchange",
        f"PROMEDIATOR_AMOUNT={SCALE.get('promediator', 1)}", "PROMEDIATOR_PREFIX=promediator",
        f"DATE_FILTER_AMOUNT={SCALE.get('date_filter', 1)}"
    ])
    add_worker("promediator", ["transactions_saver", "q3_amount_filter"], [
        "INPUT_EXCHANGE_NAME=sum_results_exchange", "OUTPUT_EXCHANGE_NAME=promediator_exchange",
        "OUTPUT_TOPIC_NAME=promediator_results_topic", f"SUM_AMOUNT={SCALE.get('sum', 1)}",
        "PROMEDIATOR_PREFIX=promediator"
    ])
    add_worker("transactions_saver", ["q3_amount_filter"], [
        "STORAGE_DIR=/storage", "INPUT_QUEUE=transactions_saver_queue",
        "INPUT_EXCHANGE_NAME=date_exchange", "INPUT_TOPIC=usd_later_period_transactions_topic",
        "OUTPUT_QUEUE=q3_amount_filter_queue", "NOTIFICATION_EXCHANGE_NAME=notification_exchange",
        "NOTIFICATION_TOPIC_NAME=notification_avgs_topic", "CONTROL_EXCHANGE_NAME=transactions_saver_control_exchange",
        "CONTROL_TOPIC_NAME=transactions_saver_control_topic", f"Q3_AMOUNT_FILTER_AMOUNT={SCALE.get('q3_amount_filter', 1)}",
        f"DATE_FILTER_AMOUNT={SCALE.get('date_filter', 1)}"
    ], volumes=["./src/transactions_saver/cache:/storage"])
    add_worker("q3_amount_filter", [], [
        "INPUT_PROMEDIATOR_EXCHANGE=promediator_exchange", "INPUT_PROMEDIATOR_TOPIC=promediator_results_topic",
        "INPUT_QUEUE=q3_amount_filter_queue", "NOTIFICATION_EXCHANGE_NAME=notification_exchange",
        "NOTIFICATION_TOPIC_NAME=notification_avgs_topic", "CONTROL_EXCHANGE_NAME=q3_amount_filter_control_exchange",
        "CONTROL_TOPIC_NAME=q3_amount_filter_control_topic", "OUTPUT_QUEUE=results_queue",
        f"PROMEDIATOR_AMOUNT={SCALE.get('promediator', 1)}", f"TRANSACTIONS_SAVER_AMOUNT={SCALE.get('transactions_saver', 1)}"
    ])

    # QUERY 4
    add_worker("group", ["bridge_matcher"], [
        "WORKER_PREFIX=group", "INPUT_QUEUE=group_queue", "INPUT_TOPIC=usd_early_period_transactions_topic",
        "INPUT_EXCHANGE_NAME=date_exchange", "OUTPUT_EXCHANGE_NAME=bridge_matcher_exchange",
        "CONTROL_EXCHANGE_NAME=group_control_exchange_name", f"NEXT_FASE_WORKERS_AMOUNT={SCALE.get('bridge_matcher', 1)}",
        "NEXT_FASE_WORKERS_PREFIX=bridge_matcher", f"DATE_FILTER_AMOUNT={SCALE.get('date_filter', 1)}", 
    ])
    add_worker("bridge_matcher", ["q4_join"], [
        "WORKER_PREFIX=bridge_matcher", "INPUT_EXCHANGE_NAME=bridge_matcher_exchange",
        "CONTROL_EXCHANGE_NAME=bridge_matcher_control_exchange", "OUTPUT_EXCHANGE_NAME=q4_join_exchange",
        f"WORKERS_AMOUNT={SCALE.get('bridge_matcher', 1)}", f"PREV_FASE_WORKERS_AMOUNT={SCALE.get('group', 1)}",
        f"NEXT_FASE_WORKERS_AMOUNT={SCALE.get('q4_join', 1)}", "NEXT_FASE_WORKERS_PREFIX=q4_join", 
    ])
    add_worker("q4_join", [], [
        "WORKER_PREFIX=q4_join", "INPUT_EXCHANGE_NAME=q4_join_exchange", "OUTPUT_QUEUE_NAME=results_queue",
        f"PREV_FASE_WORKERS_AMOUNT={SCALE.get('bridge_matcher', 1)}", 
    ])

    # QUERY 5
    add_worker("q5_date_filter", ["filter_payment_format"], [
        "INPUT_QUEUE=q5_date_filter_queue", "INPUT_EXCHANGE_NAME=transactions_exchange",
        "INPUT_TOPIC=transactions_topic", "OUTPUT_QUEUE=filter_payment_format_queue",
        f"INSTANCE_AMOUNT={SCALE.get('q5_date_filter', 1)}", "CONTROL_EXCHANGE_NAME=q5_date_filter_control"
    ])
    add_worker("filter_payment_format", ["currencies_cache"], [
        "INPUT_QUEUE=filter_payment_format_queue", "OUTPUT_QUEUE=currencies_cache",
        f"FILTER_AMOUNT={SCALE.get('filter_payment_format', 1)}", f"DATE_FILTER_AMOUNT={SCALE.get('q5_date_filter', 1)}",
        "FILTER_PAYMENT_CONTROL=filter_payment_control", "BATCH_SIZE=100"
    ])
    add_worker("currencies_cache", ["counter_q5"], [
        "INPUT_QUEUE=currencies_cache", "OUTPUT_PREFIX=counter_q5_queue",
        f"COUNTER_AMOUNT={SCALE.get('counter_q5', 1)}", f"FILTER_AMOUNT={SCALE.get('filter_payment_format', 1)}",
        f"INSTANCE_AMOUNT={SCALE.get('currencies_cache', 1)}", "CONTROL_EXCHANGE_NAME=currencies_cache_control",
        "CURRENCY_CODES_FILE=/currency_codes.json", "FALLBACK_RATES_FILE=/bitcoin_usd_rates.json",
        "EXCHANGE_RATE_API_URL=https://api.frankfurter.dev/v2/rates"
    ], volumes=["./src/currencies_cache/currency_codes.json:/currency_codes.json", "./src/currencies_cache/bitcoin_usd_rates.json:/bitcoin_usd_rates.json"])
    add_worker("counter_q5", [], [
        "INPUT_PREFIX=counter_q5_queue", "OUTPUT_QUEUE=results_queue",
        f"CACHE_AMOUNT={SCALE.get('currencies_cache', 1)}", f"INSTANCE_AMOUNT={SCALE.get('counter_q5', 1)}",
        "CONTROL_EXCHANGE_NAME=counter_q5_control"
    ])

    # ==============================================================================
    # 6. WATCHDOGS
    # ==============================================================================
    for i in range(WATCHDOG_AMOUNT):
        services[f"watchdog_{i}"] = {
            "build": {"context": "./src/", "dockerfile": "watchdog/Dockerfile"},
            "container_name": f"watchdog_{i}",
            "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "MOM_PORT=5672",
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"]
        }

    # ==============================================================================
    # 7. ESCRITURA OUTPUT
    # ==============================================================================
    compose_dict = {"services": services}
    with open(output_filename, "w") as f:
        yaml.dump(compose_dict, f, sort_keys=False, default_flow_style=False)
        print(f"Archivo generado exitosamente en: '{output_filename}'")


if __name__ == "__main__":
    main()