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
import collections
import vowpalwabbit
import numpy

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

        self.last_rps = None
        self.last_action = None
        self.last_action_p = None

        # Variables needed for tower
        self.aggregate_samples = 20;
        self.learning_rate = 0.5

    def tower(self, containers):

        # Update the samples with something new
        if (self.last_action is not None):
            #Extract last RPS and Latency
            line = subprocess.check_output(['tail', '-1', self.filename]).strip()
            latency, rps = line.decode().split(';')

            #Extract allocation
            utils.get_util_info(containers)
            for cont in containers:
                period = float(containers[cont]['period'])
                if (containers[cont]['quota'] == 'max'):
                    p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                    quota = (int(p.stdout) * period) / 1000000
                else:
                    quota = float(containers[cont]['quota']) / 1000000

            self.samples.append((
                self.last_rps,
                self.last_action,
                self.last_action_p,
                float(latency),
                quota/period,
            ))

        train_samples = list(self.samples)
        try:
            min_allocation = min(i[4] for i in train_samples if i[3] <= self.slo)
            max_allocation = max(i[4] for i in train_samples if i[3] <= self.slo)
        except ValueError:
            min_allocation = None
            max_allocation = None
        try:
            min_latency = min(i[3] for i in train_samples if i[3] > self.slo)
            max_latency = max(i[3] for i in train_samples if i[3] > self.slo)
        except ValueError:
            min_latency = None
            max_latency = None
        # Compute Cost
        for i, (rps, action, action_p, latency, allocation) in enumerate(train_samples):
            if latency <= self.slo:
                try:
                    cost = (allocation - min_allocation) / (max_allocation - min_allocation)
                except ZeroDivisionError:
                    cost = 0.5
            else:
                try:
                    cost = (latency - min_latency) / (max_latency - min_latency) + 2
                except ZeroDivisionError:
                    cost = 2.5
            train_samples[i] = (rps, action, action_p, cost)
        print("Train samples ", train_samples)

        def median(l):
            l = sorted(l)
            if not l:
                return None
            if len(l) % 2:
                return l[len(l) // 2]
            else:
                return (l[len(l) // 2 - 1] + l[len(l) // 2]) / 2

        sample_categories = collections.defaultdict(lambda: collections.defaultdict(list))
        for i in train_samples:
            action = i[1]
            rps = round(float(i[0]) / self.aggregate_samples) * self.aggregate_samples
            sample_categories[action][rps].append(i)

        # Note this is different from self.aggregate samples
        aggregated_samples = []
        for action in sample_categories:
            for rps in sample_categories[action]:
                aggregated_samples.append((rps, action, 1 / len(self.targets) ** 2, median(i[3] for i in sample_categories[action][rps])))

        train_samples = []
        if aggregated_samples:
            for i in range(1000):
                train_samples.append(random.choice(aggregated_samples))

        # RL - multi-armed bandits
        # Features are action, cost and action_p -> Against rps
        vw = vowpalwabbit.Workspace(f'--cb_explore {len(self.targets) ** 2} --epsilon 0 -l {self.learning_rate} --nn 3 --quiet')
        for rps, action, action_p, cost in train_samples:
            vw.learn(f'{action+1}:{cost}:{action_p} | rps:{rps}')

        # Predict for the current rps that it sees
        line = subprocess.check_output(['tail', '-1', self.filename]).strip()
        latency, rps = line.decode().split(';')
        distribution = vw.predict(f'| rps:{rps}')

        action = numpy.random.choice(len(distribution), p=numpy.array(distribution) / sum(distribution))
        action_p = distribution[action]

        vw.finish()

        if action_p == 1:
            distribution = [0] * len(self.targets) ** 2
            distribution[action] += 1 - self.explore
            explore_actions = []
            x = action // len(self.targets)
            y = action % len(self.targets)

            if x - 1 >= 0:
                explore_actions.append(action - len(self.targets))
            if x + 1 < len(self.targets):
                explore_actions.append(action + len(self.targets))
            if y - 1 >= 0:
                explore_actions.append(action - 1)
            if y + 1 < len(self.targets):
                explore_actions.append(action + 1)
            for i in explore_actions:
                distribution[i] += self.explore / len(explore_actions)
            action = numpy.random.choice(len(distribution), p=numpy.array(distribution) / sum(distribution))
            action_p = distribution[action]

        self.last_rps = rps
        self.last_action = action
        self.last_action_p = action_p

        self.target = self.targets[action // len(self.targets)]
        print("=====Tower predicted target: =====", self.target)


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
            latency, rps = line.decode().split(';')

            # Extract CPU limits for that container
            utils.get_util_info(containers)
            for cont in containers:
                period = float(containers[cont]['period'])
                if (containers[cont]['quota'] == 'max'):
                    p = subprocess.run(f"nproc", shell=True, capture_output=True, text=True)
                    quota = (int(p.stdout) * period) / 1000000
                else:
                    quota = float(containers[cont]['quota'])


            if (self.last_action is not None):
                self.samples.append((
                    self.last_rps,
                    self.last_action,
                    self.last_action_p,
                    float(latency),
                    quota/period,
                ))

            self.last_rps = float(rps)
            self.last_action = self.action
            self.last_action_p = action_p

            print(self.samples)



    def captain_thread(self, containers):
        while True:
            self.captain(containers)
            # self.past_alloc.append(self.limit)
            time.sleep(5)

    def autotune(self, containers):

        # # Runs every 5 seconds
        ct = threading.Thread(target=self.captain_thread, args=(containers,), daemon = True)
        ct.start()

        # Main thread logic
        while True:
            # Execute your main thread logic here
            self.tower(containers)
            time.sleep(60)



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

