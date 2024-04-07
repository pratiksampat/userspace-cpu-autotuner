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
import threading
import time
import random

## Possible throttle targets
# 0.00, 0.02, 0.04, 0.06, 0.10, 0.15, 0.20, 0.25, and 0.30


## Exploration stage ~ 6 hours app latencies may exceed SLo during this time
# Tower runs every minute to collect last minute’s (context, action, cost) sample
# Towers provide periodic support by giving out target for throttle



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

        # Tower variables
        self.file_location = 0
        self.slo = 0.3
        self.explore = 0.1
        self.drop_samples = 0
        self.aggregate_sample = 20

        self.last_action = ""
        self.past_alloc = []

        for cont in containers:
            self.history_dict[cont] = {
                'cpu_util_history': [0.0 for _ in range(5)],
                'throttle_history': [0 for _ in range(10)],
            }
            self.last_stats[cont] = {}

        self.last_scale_down = False

        self.samples = []

        # Variables needed for warmup
        self.targets = [0.0, 0.02, 0.04, 0.06, 0.1, 0.15, 0.2, 0.25, 0.3]
        self.stats = {}
        self.explore_count = {i: 0 for i in range(len(self.targets) ** 2)}
        self.warmup = 3 # 3 minutes of warmup
        self.stage = -self.warmup - 1;
        self.action = None
        self.filename = "/home/docker/wrk2/record.log"

    def warmup_cont(self, containers):
        # Don't do anything for warmup amount of time
        if (self.stage < 0):
            print("Idle phase")
            time.sleep(60)
            self.stage += 1
            return

        # When the warmup has ended,
        # explore the action and choose a target based on that
        if (self.stage == 0):
            print("Warm phase ended")
            self.stage = 1
            min_explore_count = min(self.explore_count.values())
            actions_with_min_explore_count = [k for k, v in self.explore_count.items() if v == min_explore_count]
            self.action = random.choice(actions_with_min_explore_count)
            self.explore_count[self.action] += 1
            target_throttle = self.targets[self.action // len(self.targets)]
            self.target = target_throttle
            print("Action:Target=", self.action, self.target)

            # Chosen action will be executed for 2 minutes
            self.captain(containers)
            time.sleep(120)

        # Update the state
        if (self.stage == 1):
            self.stage = 0
            action_p = 1 / len(self.targets) ** 2

            #Extract last RPS and Latency
            line = subprocess.check_output(['tail', '-1', self.filename]).strip()
            latency, rps = line.decodes.split(';')

            # Extract CPU limits for that container
            utils.get_util_info(containers)
            for cont in containers:
                period = float(containers[cont]['period'])
                if (containers[cont]['quota'] == 'max'):
                    p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                    quota = (int(p.stdout) * period) / 1000000
                else:
                    quota = float(containers[cont]['quota']) / 1000000

            self.samples.append((
                float(rps),
                self.action,
                action_p,
                float(latency),
                quota/period,
            ))

            print(self.samples)



    # def captain_thread(self, containers):
    #     while True:
    #         self.captain(containers)
    #         self.past_alloc.append(self.limit)
    #         time.sleep(5)

    def autotune(self, containers):
        pass
        # Executes every N=10 periods. TODO: Run this in background
        # while True:
        #     self.captain(containers)
        #     # os.sleep(1) # 10 periods of 100 ms each

    #     # Runs every second - docker stats takes approx 1 second to finish
    #     ct = threading.Thread(target=self.captain_thread, args=(containers,), daemon = True)
    #     ct.start()

    #     # Main thread logic
    #     while True:
    #         # Execute your main thread logic here
    #         self.tower(containers)
    #         time.sleep(60)


    # def get_rps_stats(self, file_path, file_position):
    #     rps_list = []

    #     try:
    #         with open(file_path, 'r') as file:
    #             file.seek(file_position)
    #             # Read remaining lines and get second column information
    #             for line in file:
    #                 columns = line.strip().split(';')
    #                 if len(columns) >= 2:
    #                     rps_list.append(float(columns[0]))

    #             # Save the current file position
    #             file_position = file.tell()
    #     except:
    #         print("Nothing more to read")

    #     return rps_list, file_position

    # def greater_than_slo(self, x):
    #     return x > self.slo


    def tower(self, containers):
        pass
        # train_samples = []
        # latency_list, self.file_location = self.get_rps_stats("record.log", self.file_location)
        # print(latency_list)
        # print(self.file_location)

        # latency_mean = statistics.mean(latency_list)
        # latency_99 = statistics.quantiles(latency_list, n=100)[-1]
        # print(latency_mean, latency_99)

        # # Get SLO violation list
        # latency_violation = list(filter(self.greater_than_slo, latency_list))
        # latency_min = min(latency_violation)
        # latency_max = max(latency_violation)
        # alloc_min = min(self.past_alloc)
        # alloc_max = max(self.past_alloc)

        # for lat in latency_list:
            
        # # Get Cost

        # train_samples.append({rps, action, action_p, cost})

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

