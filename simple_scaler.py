import utils
import pprint
import subprocess
import statistics

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
                # 'location': containers[cont]['cgroup_loc'],
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

    # Simple recommend logic - If load > 90 percent step scale up,
    # otherwise step down until we have an average utilization over 50P
    def recommend(self, containers):
        for cont in containers:
            load_rate = statistics.mean(self.history_dict[cont]['cpu_load_history'])

            period = float(containers[cont]['period'])
            if (containers[cont]['quota'] == 'max'):
                p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                old_quota = int(p.stdout) * period
            else:
                old_quota = float(containers[cont]['quota'])

            quota = old_quota
            if (load_rate >= 90):
                quota = old_quota + 5000
            elif (load_rate <= 50):
                if (old_quota - 5000 > 10000):
                    quota = old_quota - 5000

            if (quota != old_quota):
                utils.update_quota_period(containers[cont]['cgroup_loc'], quota)
                print("Recommending quota - ", quota)
            else:
                print("No change in recommendation")

            del self.history_dict[cont]['cpu_util_history'][:]
            del self.history_dict[cont]['cpu_load_history'][:]
            del self.history_dict[cont]['throttled_percent_history'][:]
            del self.history_dict[cont]['quota_history'][:]
            del self.history_dict[cont]['period_history'][:]
