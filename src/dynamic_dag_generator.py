import json

from jinja2 import Environment, FileSystemLoader
import argparse
import os
import requests
import logging

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template("resources/dag_template.jinja2")


def createDag(dag_name, dag_id, dag_path, schedule_interval, catchup=False):
    try:
        with open(f"{dag_path}/{dag_name}_{dag_id}.py", "w") as f:
            f.write(
                template.render(dag_id=dag_id, dag_name=dag_name, schedule_interval=schedule_interval, catchup=catchup))
            logger.info(f"[+] A new DAG with id: {dag_name}_{dag_id} created successfully")
    except:
        logger.error(f"[-] Failed to create DAG having id: {dag_name}_{dag_id} ")


def deleteDag(dag_name, dag_id):
    res = requests.delete(f'http://localhost:8080/api/v1/dags/{dag_name}_{dag_id}', auth=("airflow", "airflow"),
                          verify=True)
    if os.path.exists(f"dags/{dag_name}_{dag_id}.py"):
        os.remove(f"dags/{dag_name}_{dag_id}.py")
    if res.status_code == 204:
        logger.info(f"[+]Deleted DAG {dag_name}_{dag_id}.py")
    else:
        logger.error(f"[-]Failed to delete DAG {dag_name}_{dag_id}.py")


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
    parser.add_argument('--action', type=str, required=True)
    parser.add_argument('--dag-name', type=str, required=True)
    parser.add_argument('--dag-id', type=str, required=True)
    parser.add_argument('--dag-path', type=str, required=False, default="/dags")
    parser.add_argument('--schedule-interval', type=str, required=False, default='@Daily')
    parser.add_argument('--catchup', type=str, required=False, default=False)
    parser.add_argument('--payload', type=str, required=False, default='{}')
    args = parser.parse_args()

    if args.action == 'create':
        logger.info(f"[+] Provided action: {args.action}")
        createDag(args.dag_name, args.dag_id, args.dag_path, args.schedule_interval, args.catchup)
    elif args.action == 'update':
        pass
    ##  updateDag(args.dag_name, args.dag_id, args.payload)
    elif args.action == 'delete':
        logger.info(f"[+] Provided action: {args.action}")
        deleteDag(args.dag_name, args.dag_id)
