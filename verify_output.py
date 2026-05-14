import csv
import yaml
import logging
import subprocess

DOCKER_FILE_PATH = "./docker-compose.yaml"

# modificar en caso de no querer testearlos
CHECK_QUERY1 = True
CHECK_QUERY2 = True
CHECK_QUERY3 = True
CHECK_QUERY4 = True
CHECK_QUERY5 = True

# constantes a meter en el protocolo o a modificar una vez implementado el mismo
RESPONSE_TYPE = 0
QUERY1_RESPONSE = "1"
QUERY2_RESPONSE = "2"
QUERY3_RESPONSE = "3"
QUERY4_RESPONSE = "4"
QUERY5_RESPONSE = "5"

ID = 1
RESPONSE = 1

QUERY5_RESULT = '37812'

class ClientValidationError(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def await_client_containers(client_services_name):
    result = subprocess.run(
        ["docker", "container", "wait"] + client_services_name, capture_output=True
    )

    zero_exit_code_count = 0
    for char in result.stdout.decode("utf-8"):
        if char == "0":
            zero_exit_code_count += 1

    if zero_exit_code_count != len(client_services_name):
        raise ClientValidationError("One or more clients exited with an error code")


def find_environment_variable(environment_variables, target_environment_variable):
    for environment_variable in environment_variables:
        [name, value] = environment_variable.split("=")
        if name == target_environment_variable:
            return value
    return None

def build_input():
    query1 = set()
    query2 = set()
    query3 = set()
    query4 = set()
    query5 = QUERY5_RESULT
    #query 1
    try:
        if CHECK_QUERY1:
            with open("./datasets/results_query1.csv", newline="\n") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                next(csv_reader)
                for row in csv_reader:
                    query1.add(row[ID])
    except Exception as e:
        logging.error(e)
        raise ClientValidationError("Couldn't read input file for query 1")
    #query 2
    try:
        if CHECK_QUERY2:
            with open("./datasets/results_query2.csv", newline="\n") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                next(csv_reader)
                for row in csv_reader:
                    query2.add(row[ID])
    except Exception as e:
        logging.error(e)
        raise ClientValidationError("Couldn't read input file for query 2")
    #query 3
    try:
        if CHECK_QUERY3:
            with open("./datasets/results_query3.csv", newline="\n") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                next(csv_reader)
                for row in csv_reader:
                    query3.add(row[ID])
    except Exception as e:
        logging.error(e)
        raise ClientValidationError("Couldn't read input file for query 3")
    #query 4
    try:
        if CHECK_QUERY4:
            with open("./datasets/results_query4.csv", newline="\n") as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
                next(csv_reader)
                for row in csv_reader:
                    query4.add((row[1], row[2]))
    except Exception as e:
        logging.error(e)
        raise ClientValidationError("Couldn't read input file for query 4")

    return query1, query2, query3, query4, query5
                


def read_output(output_file):
    try:
        query1 = set()
        query2 = set()
        query3 = set()
        query4 = set()
        query5 = 0
        with open(output_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            for row in csv_reader:
                if row[RESPONSE_TYPE] == QUERY1_RESPONSE: # estas constantes tendrian que salir del protocolo
                    query1.add(row[ID])
                if row[RESPONSE_TYPE] == QUERY2_RESPONSE:
                    query2.add(row[ID])
                if row[RESPONSE_TYPE] == QUERY3_RESPONSE:
                    query3.add(row[ID])
                if row[RESPONSE_TYPE] == QUERY4_RESPONSE:
                    query4.add((row[1], row[2]))
                if row[RESPONSE_TYPE] == QUERY5_RESPONSE:
                    query5 = row[RESPONSE]

    except Exception as e:
        logging.error(e)
        raise ClientValidationError("Couldn't read output file")
    return query1, query2, query3, query4, query5

def compare(received,expected,query_number):

    cant_received = len(received)
    cant_expected = len(expected)

    if cant_received != cant_expected:
        logging.info(f"Query {query_number} Error: received and expected containers have different lengths")
        logging.info(f"Received answers length: {cant_received} - Expected answers length: {cant_expected}")
        return

    for transaction in received:
        if transaction not in expected:
            logging.info(f"Error Query {query_number}: {transaction} is not in the expected results")
            return

    logging.info(f"Query {query_number} OK")

def verify_client_output():
    #def verify_client_output(client_service): descomentar
    #client_name = client_service["container_name"] descomentar
    # logging.info(client_name) descomentar
    #environment = client_service["environment"] descomentar

    #output_file = "." + find_environment_variable(environment, "OUTPUT_FILE") descomentar
    output_file = "./datasets/output.csv"

    # environment = client_service["environment"] descomentar

    if not output_file:
        raise ClientValidationError("Bad file environment variable config")

    expected_results = build_input()
    received_results = read_output(output_file)

    if CHECK_QUERY1:
        compare(received_results[0], expected_results[0],1)
    if CHECK_QUERY2:
        compare(received_results[1], expected_results[1],2)
    if CHECK_QUERY3:
        compare(received_results[2], expected_results[2],3)
    if CHECK_QUERY4:
        compare(received_results[3], expected_results[3],4)

    if CHECK_QUERY5:
        if received_results[4] == expected_results[4]:
            logging.info("OK, Query 5 matched the expected result")
        else:
            logging.info(f"Error Query 5, Received: {received_results[4]} - Expected: {expected_results[4]}")


def main():
    logging.basicConfig(level=logging.INFO)

    try:
        with open(DOCKER_FILE_PATH, "r") as docker_compose_file:
            """
            parsed_docker_compose_file = yaml.safe_load(docker_compose_file)
            services = parsed_docker_compose_file["services"]
            client_services_name = list(
                filter(
                    lambda service_key: "client"
                    in services[service_key]["build"]["dockerfile"],
                    services.keys(),
                )
            )

            logging.info("Awaiting client containers to exit...")
            await_client_containers(client_services_name)
            logging.info("Validating clients...")
            for client_service_name in client_services_name:
                client_service = services[client_service_name]
                verify_client_output(client_service)
            """
            verify_client_output() # a quitar
            
    except ClientValidationError as e:
        logging.error(e.message)
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    main()