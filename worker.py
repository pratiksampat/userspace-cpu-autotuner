## Worker python program that extracts K8s CPU related metrics

import argparse
import pathlib
import subprocess

def get_pod_information(pods):
    uid_map = {}

    p = subprocess.run(['kubectl', 'get', 'pods', f'-n=default',
                        r'-o=jsonpath={range .items[*]}{.metadata.uid} {.metadata.name}{"\n"}{end}'],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    for i in p.stdout.splitlines():
        uid, name = i.split()
        name = name.rsplit('-', 2)[0]
        if len(pods) != 0:
            if name in pods:
                uid_map[uid] = {'name': name}
        else:
            uid_map[uid] = {'name': name}

        print(uid_map)

    # Get cgroup location of the UIDs
    for uid in uid_map:
        regex_uid = str(uid.split('-')[0])
        print(type(regex_uid))
        r_uid = "*" + regex_uid + "*"
        p = subprocess.run(['find', '/sys/fs/cgroup', '-type', 'd', '-name', r_uid],
                           stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
        uid_map[uid]['cgroup'] = p.stdout.replace("\n", "")

    print(uid_map)

def main():
    parser = argparse.ArgumentParser(description='Process a list')
    parser.add_argument('--pods', type=str, help='List to process (enclosed in square brackets)')
    args = parser.parse_args()
    pod_list = []
    try:
        if args.pods:
            pod_list = list(args.pods.split(" "))
    except ValueError:
        print("Error: Please provide a valid list enclosed in square brackets.")
        return

    get_pod_information(pod_list)


if __name__ == "__main__":
    main()