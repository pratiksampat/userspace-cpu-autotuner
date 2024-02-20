import utils
import pprint
import subprocess

class SimpleScaler:
    def __init__(self, containers):
        print("Initting")
        self.pp = pprint.PrettyPrinter(depth=4)
        self.history_dict = {}

        for cont in containers:
            self.history_dict[cont] = {
                'cpu_util_history': [],
                'cpu_load_history': [],
                'throttled_percent_history': [],
                'quota_history': [],
                'period_history': [],
                'location': containers[cont]['cgroup_loc'],
            }

    def autotune(self, containers):
        for i in range(5):
            self.learn(containers)
        self.pp.pprint(self.history_dict)
        self.recommend(containers)

    def learn(self, containers):
        print("Learning")
        utils.get_util_info(containers)
        # self.pp.pprint(containers)

        for cont in containers:
            period = float(containers[cont]['period'])
            if (containers[cont]['quota'] == 'max'):
                p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                quota = int(p.stdout) * period
            else:
                quota = float(containers[cont]['quota'])

            limits = quota / period
            util = float(containers[cont]['cpu_util'].replace('%', ''))
            load = util / limits
            throttled_per = (containers[cont]['nr_throttled'] / containers[cont]['nr_periods']) * 100

            self.history_dict[cont]['cpu_util_history'].append(util)
            self.history_dict[cont]['cpu_load_history'].append(load)
            self.history_dict[cont]['throttled_percent_history'].append(throttled_per)
            self.history_dict[cont]['quota_history'].append(quota)
            self.history_dict[cont]['period_history'].append(period)

    def recommend(self, containers):
        print("Recommending")
