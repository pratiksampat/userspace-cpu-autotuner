import subprocess

def extract_throttle_stats(fi):
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
        stats = extract_throttle_stats(cont_map[cont]['cgroup_loc'])
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