## Worker python program that extracts K8s CPU related metrics

import subprocess

def get_pod_information():
    p = subprocess.run(['kubectl', 'get', 'pods', f'-n=default',
                        r'-o=jsonpath={range .items[*]}{.metadata.uid} {.metadata.name}{"\n"}{end}'],
                        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, check=True)
    for i in p.stdout.splitlines():
        uid, name = i.split()
        name = name.rsplit('-', 2)[0]
        print(uid, name)

def main():
    print("Hello world")
    get_pod_information()


if __name__ == "__main__":
    main()