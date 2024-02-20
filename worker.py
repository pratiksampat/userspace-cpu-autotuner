## Worker python program that extracts K8s CPU related metrics

import argparse
import subprocess
from simple_scaler import SimpleScaler

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

def extract_stats(fi):
    stats = {}
    filename = fi + '/' + 'cpu.stat'
    with open(filename, 'r') as file:
        for line in file:
            parts = line.split()
            if len(parts) == 2:
                key, value = parts
                stats[key] = float(value)
    return stats

def extract_quota_period(fi):
    filename = fi + '/' + 'cpu.max'
    quota = -1
    period = -1

    with open(filename, 'r') as file:
        content = file.read().strip()
        if content:
            quota, period = content.split()
    return quota, period

def get_util_info(cont_map):
    for cont in cont_map:
        # CPU Util info
        command = f"docker stats --no-stream | grep {cont} | awk '{{print $3}}'"
        p = subprocess.run(command, shell=True, capture_output=True, text=True)
        p.stdout.replace("%", "")
        cont_map[cont]['cpu_util'] = p.stdout.replace("\n", "")

        #Throttle info in cpu.stat file
        stats = extract_stats(cont_map[cont]['cgroup_loc'])
        cont_map[cont]['nr_periods'] = stats.get('nr_periods', 0)
        cont_map[cont]['nr_throttled'] = stats.get('nr_throttled', 0)
        cont_map[cont]['throttled_usec'] = stats.get('throttled_usec', 0)

        # Get current period and quota
        quota, period = extract_quota_period(cont_map[cont]['cgroup_loc'])
        cont_map[cont]['quota'] = quota
        cont_map[cont]['period'] = period

def update_quota_period(f, quota, period=100000):
    filename = f + '/' + 'cpu.max'
    content = str(quota) + " " + str(period)
    with open(filename, 'w') as file:
        file.write(content)

## Pass containers with the prefix k8s_<container_name>
def main():
    parser = argparse.ArgumentParser(description='Process a list')
    parser.add_argument('--containers', type=str, help='List to process (enclosed in square brackets)')
    parser.add_argument('--scaler', type=str, help='simple, lstm, firm')
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
    print(containers)

    get_util_info(containers)
    print(containers)

    # Setup an autoscaler to observe and recommend
    scaler_classes = {
        'simple': SimpleScaler,
    }

    if (args.scaler in scaler_classes):
        scaler = scaler_classes[args.scaler]()
    else:
        print("Scaler not found. Defaulting to simple")
        scaler = scaler_classes['simple']
    scaler.learn()
    scaler.recommend()


    # # Test to verify updating of quota-period
    # for cont in containers:
    #     update_quota_period(containers[cont]['cgroup_loc'], "max")


if __name__ == "__main__":
    main()