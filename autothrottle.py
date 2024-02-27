# Implementation of the MSFT autothrottle: https://github.com/microsoft/autothrottle

## Captain notes
"""
Each Captain periodically (e.g., every minute) receives a target CPU
throttle ratio from the Tower
throttle ratio = throttle_count / nr_periods
CPU usage also used
"""

## Tower

import utils
import pprint
import subprocess
import statistics
import os

## Possible throttle targets
# 0.00, 0.02, 0.04, 0.06, 0.10, 0.15, 0.20, 0.25, and 0.30

class AutoThrottle:
    def __init__(self, containers):
        print("Initting")
        self.pp = pprint.PrettyPrinter(depth=4)
        self.history_dict = {}
        self.last_stats = {}
        self.target = 0.06
        self.new_quota = 1
        self.margin = 3
        self.scale_down_cd = 0

        for cont in containers:
            self.history_dict[cont] = {
                'cpu_util_history': [0.0 for _ in range(5)],
                'throttle_history': [0 for _ in range(10)],
            }
            self.last_stats[cont] = {}

        self.last_scale_down = False

    def autotune(self, containers):
        # Executes every N=10 periods. TODO: Run this in background
        while True:
            self.captain(containers)
            # os.sleep(1) # 10 periods of 100 ms each

    def captain(self, containers):
        print("Captain learning")
        utils.get_util_info(containers)
        for cont in containers:
            period = float(containers[cont]['period'])
            if (containers[cont]['quota'] == 'max'):
                p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                quota = (int(p.stdout) * period) / 1000000
            else:
                quota = float(containers[cont]['quota']) / 1000000
            util = float(containers[cont]['cpu_util'].replace('%', ''))

            # If nothing in the last stats fill that first and break
            if len(self.last_stats[cont]) == 0:
                self.last_stats[cont]['cpu_util_history'] = util
                self.last_stats[cont]['nr_throttled'] = containers[cont]['nr_throttled']
                self.last_stats[cont]['quota'] = quota
                self.new_quota = quota
                break

            # Now that we have the last stats
            throttleCount = containers[cont]['nr_throttled'] - self.last_stats[cont]['nr_throttled']
            self.history_dict[cont]['throttle_history'].append(throttleCount)
            self.history_dict[cont]['throttle_history'].pop(0)
            # usage = containers[cont]['cpu_util_history'] - self.last_stats[cont]['cpu_util_history']
            self.history_dict[cont]['cpu_util_history'].append(util)
            self.history_dict[cont]['cpu_util_history'].pop(0)

            ## Update the last stats
            self.last_stats[cont]['cpu_util_history'] = util
            self.last_stats[cont]['nr_throttled'] = containers[cont]['nr_throttled']


            throttled_rate = statistics.mean(self.history_dict[cont]['throttle_history']) / 10

            print("Throttled Rate0: ", throttled_rate, "Target: ", self.target)

            if throttled_rate > 3 * self.target and self.last_scale_down:
                self.new_quota = 2 * self.last_stats[cont]['quota'] - self.new_quota
                self.margin += (throttled_rate - self.target)
                self.history_dict[cont]['throttle_history'] = [0 for _ in range(10)]
                self.last_scale_down = False
                print("Scale up")

            self.last_stats[cont]['quota'] = self.new_quota
            throttled_rate = statistics.mean(self.history_dict[cont]['throttle_history']) / 10
            usage_max = max(self.history_dict[cont]['cpu_util_history'])
            usage_std = statistics.stdev(self.history_dict[cont]['cpu_util_history'])
            self.margin += (throttled_rate - self.target)
            self.margin = max(0, self.margin)
            self.last_scale_down = False

            print("Throttled Rate1: ", throttled_rate, "Target: ", self.target)
            print("Usage max: ", usage_max, "usage_std: ", usage_max)
            if throttled_rate > 3 * self.target:
                self.new_quota *= 1 + (throttled_rate - 3 * self.target)
                print("Scale up")
            else:
                usage_limit = usage_max + usage_std * self.margin
                # print("Usage limit: ", usage_limit / 1000)
                if usage_limit/1000 <= self.new_quota * 0.9:
                    self.new_quota = max(self.new_quota * 0.5, usage_limit/1000)
                    self.last_scale_down = True
                    print("Scale down")
            self.history_dict[cont]['throttle_history'] = [0 for _ in range(10)]
            self.new_quota = max(0.01, self.new_quota)

            if (self.new_quota >= 4.8):
                self.new_quota = 4.8

            print("Recommended Quota - ", self.new_quota)
            quota_in_us = self.new_quota * 1000000
            utils.update_quota_period(containers[cont]['cgroup_loc'], quota_in_us)

