import json

from jinja2 import Environment, FileSystemLoader
import argparse
import os
import requests
import logging

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template("resources/dag_template.jinja2")


def createDag(dag_payload, airflow_profile):
    dag_name = dag_payload['dag_name']
    dag_id = dag_payload['dag_id']
    connection_details = dag_payload['connection_details']
    try:
        # create a DAG connection before creating the DAG
        if dag_payload.has_key("connection"):
            createDAGConnection(connection_details, airflow_profile)
        with open(f"{airflow_profile['dag_path']}/{dag_name}_{dag_id}.py", "w") as f:
            f.write(template.render(payload=payload))
            logger.info(f"[+] A new DAG with id: {dag_name}_{dag_id} created successfully")
    except:
        logger.error(f"[-] Failed to create DAG having id: {dag_name}_{dag_id} ")


def createDAGConnection(dag_connection_payload, resources):
    dag_conn_profile = json.loads(dag_connection_payload)
    res = requests.post(
        f"http://{resources['airflow_instance_host']}:{resources['airflow_instance_port']}/api/v1/connections/",
        auth=(resources['airflow_instance_user'], resources['airflow_instance_pass']), data=dag_conn_profile,
        verify=True)
    if res.status_code == 200:
        logger.info(f"[+] Created DAG connection with name: {dag_conn_profile['connection_id']} successfully")
    else:
        logger.info(f"[+] Failed to create connection: {dag_conn_profile['connection_id']} ")


def deleteDag(dag_payload, resources):
    dag_name = dag_payload['dag_name']
    dag_id = dag_payload['dag_id']
    res = requests.delete(
        f"http://{resources['airflow_instance_host']}:{resources['airflow_instance_port']}/api/v1/connections/",
        auth=(resources['airflow_instance_user'], resources['airflow_instance_pass']),
        verify=True)
    if os.path.exists(f"{resources['dag_path']}/{dag_name}_{dag_id}.py"):
        os.remove(f"{resources['dag_path']}/{dag_name}_{dag_id}.py")
    if res.status_code == 204:
        logger.info(f"[+] Deleted DAG {dag_name}_{dag_id}.py successfully")
    else:
        logger.error(f"[-] Failed to delete DAG {dag_name}_{dag_id}.py")


def getLogger(name):
    log_format = '%(asctime)s %(name)5s %(levelname)5s       %(message)s'
    logging.basicConfig(level=logging.ERROR,
                        format=log_format,
                        filemode='w')
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(log_format))
    logging.getLogger(name).setLevel(logging.INFO)
    return logging.getLogger(name)


if __name__ == "__main__":
    logger = getLogger("Dynamic Dag generator")
    parser = argparse.ArgumentParser()
    parser.add_argument('--resources-path', type=str, required=True)
    parser.add_argument('--payload', type=str, required=True)
    args = parser.parse_args()

    with open('resources/dag_generator.json') as json_file:
        airflow_conn_profile = json.load(json_file)

    if args.payload is not None:
        payload = json.loads(args.payload)
        if payload['action'] == 'create':
            logger.info(f"[+] Provided action: Create")
            createDag(payload, airflow_conn_profile)
        elif payload['action'] == 'delete':
            logger.info(f"[+] Provided action: Delete")
            deleteDag(payload, airflow_conn_profile)
    else:
        raise Exception("payload is not provided")
