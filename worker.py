## Worker python program that extracts K8s CPU related metrics

import argparse
import subprocess
from simple_scaler import SimpleScaler
from lstm_hw_scaler import LSTM_HW_Scaler
from autothrottle import AutoThrottle
import utils

def get_container_information(container_names):
    cont_map = {}
    for container in container_names:
        p = subprocess.run(['docker', 'ps', '--filter', 'name='+container, '--format', '{{.ID}}'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
        id = p.stdout.replace("\n", "")
        print(id)
        cont_map[id] = {'name': container}
        r_uid = "*" + id + "*"
        p = subprocess.run(['find', '/sys/fs/cgroup', '-type', 'd', '-name', r_uid],
                           stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
        cont_map[id]['cgroup_loc'] = p.stdout.replace("\n", "")

    return cont_map

## Pass containers with the prefix k8s_<container_name>
def main():
    parser = argparse.ArgumentParser(description='Process a list')
    parser.add_argument('--containers', type=str, help='List to process with spaces')
    parser.add_argument('--scaler', type=str, help='simple, lstm, autothrottle')
    args = parser.parse_args()
    cont_list = []
    try:
        if args.containers:
            cont_list = list(args.containers.split(" "))
    except ValueError:
        print("Error: Please provide a valid list enclosed in square brackets.")
        return

    # Get all IDs and Cgroup locations of the containers
    containers = get_container_information(cont_list)

    utils.get_util_info(containers)

    # Setup an autoscaler to observe and recommend
    scaler_classes = {
        'simple': SimpleScaler(containers),
        'autothrottle': AutoThrottle(containers),
        'lstm' : LSTM_HW_Scaler(containers, args.scaler),
        'hw' : LSTM_HW_Scaler(containers, args.scaler)
    }

    if (args.scaler in scaler_classes):
        scaler = scaler_classes[args.scaler]
    else:
        print("Scaler not found. Defaulting to simple")
        scaler = scaler_classes['simple']

    # while True:

    # Autothrottle needs a warmup of 3 minutes phases of 6 hours
    for i in range(10):
        if (args.scaler == 'autothrottle'):
            scaler.warmup_cont(containers)

    scaler.autotune(containers)


    # # Test to verify updating of quota-period
    # for cont in containers:
    #     update_quota_period(containers[cont]['cgroup_loc'], "max")


if __name__ == "__main__":
    main()