# LSTM implementation from: https://github.com/ZhaoNeil/On-Demand-Resizing/blob/master/serverless-deploy/coldstart.py


# HW - Holt-Winters exponential smoothing
# LSTM - Long Short-Term Memory

import utils
import pprint
import subprocess
import numpy as np
import math
import time
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.api import ExponentialSmoothing
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM

class LSTM_HW_Scaler:
    def __init__(self, containers, scaler):
        print("Initting")
        self.pp = pprint.PrettyPrinter(depth=4)
        self.scaler = scaler

        self.history_dict = {}

        for cont in containers:
            self.history_dict[cont] = {
                'cpu_util_history': [],
                'cpu_load_history': [],
                'throttled_percent_history': [],
                'quota_history': [],
                'period_history': [],
            }

    def autotune(self, containers):
        while True:
            for i in range(200):
                print("Learning: ", i)
                self.learn(containers)
            self.pp.pprint(self.history_dict)

            self.recommend(containers)
            time.sleep(180)

    def learn(self, containers):
        utils.get_util_info(containers)

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
            if (containers[cont]['nr_periods'] == 0):
                throttled_per = 0
            else:
                throttled_per = (containers[cont]['nr_throttled'] / containers[cont]['nr_periods']) * 100

            self.history_dict[cont]['cpu_util_history'].append(util)
            self.history_dict[cont]['cpu_load_history'].append(load)
            self.history_dict[cont]['throttled_percent_history'].append(throttled_per)
            self.history_dict[cont]['quota_history'].append(quota)
            self.history_dict[cont]['period_history'].append(period)

    def recommend(self, containers):
        for cont in containers:
            hw_upper, hw_lower, hw_target, lstm_upper, lstm_lower, lstm_target = HW_LSTM(self.history_dict[cont]['cpu_util_history'])
            print("HW upper, lower , target", hw_upper, hw_lower, hw_target * 1000)
            print("LSTM upper, lower, target", lstm_upper, lstm_lower, lstm_target * 1000)

            # hw_upper_avg = statistics.mean(hw_upper)
            # lstm_upper_avg = statistics.mean(lstm_upper)
            # print("HW upper avg", hw_upper_avg)
            # print("LSTM upper avg", lstm_upper_avg)

            if self.scaler == 'hw':
                utils.update_quota_period(containers[cont]['cgroup_loc'], hw_upper)
            else:
                utils.update_quota_period(containers[cont]['cgroup_loc'], lstm_upper)

            del self.history_dict[cont]['cpu_util_history'][:]
            del self.history_dict[cont]['cpu_load_history'][:]
            del self.history_dict[cont]['throttled_percent_history' ][:]
            del self.history_dict[cont]['quota_history'][:]
            del self.history_dict[cont]['period_history'][:]


def split_sequence(sequence, n_steps_in, n_steps_out, ywindow):
    X, y = list(), list()

    for i in range(len(sequence)-ywindow-n_steps_in+1):
        # find the end of this pattern
        end_ix = i + n_steps_in

        # gather input and output parts of the pattern
        # print(sequence[end_ix:end_ix+ywindow])
        seq_x, seq_y = sequence[i:end_ix], [np.percentile(sequence[end_ix:end_ix+ywindow], 90), np.percentile(sequence[end_ix:end_ix+ywindow], 60), np.percentile(sequence[end_ix:end_ix+ywindow], 98)]
        X.append(seq_x)
        y.append(seq_y)

    return np.array(X), np.array(y)

def trans_forward(arr):
    global scaler
    out_arr = scaler.transform(arr.reshape(-1, 1))
    return out_arr.flatten()

def trans_back(arr):
    global scaler
    out_arr = scaler.inverse_transform(arr.flatten().reshape(-1, 1))
    return out_arr.flatten()

def create_lstm(n_steps_in, n_steps_out, n_features,raw_seq, ywindow):
    global scaler
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler = scaler.fit(raw_seq.reshape(-1, 1))
    #print("First 10 of raw_seq:", raw_seq[:20])
    dataset = trans_forward(raw_seq)
    # split into samples
    X, y = split_sequence(dataset, n_steps_in, n_steps_out, ywindow)
    # reshape from [samples, timesteps] into [samples, timesteps, features]
    X = X.reshape((X.shape[0], X.shape[1], n_features))
    # define model
    model = Sequential()

    # Multi-layer model
    model.add(LSTM(50, return_sequences=True , input_shape=(n_steps_in, n_features)))
    # model.add(LSTM(50, return_sequences=True))
    model.add(LSTM(50))

    # Single layer model
    # model.add(LSTM(100, input_shape=(n_steps_in, n_features)))

    model.add(Dense(n_steps_out))
    model.compile(optimizer='adam', loss='mse')
    # fit model
    model.fit(X, y, epochs=10, verbose=0)

    return model

def lstm_predict(input_data,model,n_steps_in,n_features):
    x_input = np.array(trans_forward(input_data))
    x_input = x_input.reshape((1, n_steps_in, n_features))
    yhat = model.predict(x_input, verbose=0)
    return trans_back(yhat)

def calc_n(i, season_len, history_len):
    season = math.ceil((i+1)/season_len)
    history_start_season = season - (history_len/season_len)
    if history_start_season < 1:
        history_start_season = 1
    history_start = (history_start_season-1) * season_len
    n = int(i - history_start)
    return n

def HW_LSTM(series):
    HW_target, HW_upper, HW_lower = 90, 98, 60
    lstm_target, lstm_upper, lstm_lower = 90, 98, 60

    season_len = 60
    history_len = season_len * 3
    scaling_start_index = season_len * 2

    window_past = 1
    window_future = 24

    i = scaling_start_index
    rescale_buffer = 120
    rescale_cooldown = 18
    lstm_cooldown = 0
    hw_cooldown = 0

    model = None
    hw_model = None
    steps_in, steps_out, n_features, ywindow = 48, 3, 1, 24

    lstm_CPU_request = 250
    hw_CPU_request =250

    lstm_requests = [lstm_CPU_request] * scaling_start_index
    lstm_targets = []
    lstm_uppers = []
    lstm_lowers = []

    hw_requests = [hw_CPU_request] * scaling_start_index
    hw_targets = []
    hw_uppers = []
    hw_lowers = []

    print("Here", len(series))

    while i <= len(series):
        series_part = series[:i]
        n = calc_n(i, season_len, history_len)

        #HW model
        if i % 60 == 0 or hw_model is None:
            hw_model = ExponentialSmoothing(series_part[-n:], trend="add", seasonal="add", seasonal_periods=season_len)
            model_fit = hw_model.fit()

        hw_window = model_fit.predict(start=n-window_past,end=n+window_future)
        hw_target = np.percentile(hw_window, HW_target)
        hw_lower = np.percentile(hw_window, HW_lower)
        hw_upper = np.percentile(hw_window, HW_upper)
        if hw_target < 0:
            hw_target = 0
        if hw_lower < 0:
            hw_lower = 0
        if hw_upper < 0:
            hw_upper = 0
        hw_targets.append(hw_target)
        hw_uppers.append(hw_upper)
        hw_lowers.append(hw_lower)

        # print("Hardware target", hw_targets)
        # print("Hardware upper", hw_uppers)
        # print("Hardware lower", hw_lowers)

        #LSTM model
        if i % 60 == 0 or model is None:
            model = create_lstm(steps_in, steps_out, n_features, np.array(series_part), ywindow)

        input_data = np.array(series_part[-steps_in:])
        output_data = lstm_predict(input_data, model, steps_in, n_features)

        lstm_target = output_data[0]
        lstm_lower = output_data[1]
        lstm_upper = output_data[2]
        if lstm_target < 0:
            lstm_target = 0
        if lstm_lower < 0:
            lstm_lower = 0
        if lstm_upper < 0:
            lstm_upper = 0
        lstm_targets.append(lstm_target)
        lstm_uppers.append(lstm_upper)
        lstm_lowers.append(lstm_lower)

        # print("LSTM target", lstm_targets)
        # print("LSTM upper", lstm_uppers)
        # print("LSTM lower", lstm_lowers)
        # HW scaling
        hw_CPU_request_unbuffered = hw_CPU_request - rescale_buffer
        # If no cool-down
        if (hw_cooldown == 0):
            # If request change greater than 50
            if (abs(hw_CPU_request - (hw_target + rescale_buffer)) > 50):
                # If above upper
                if hw_CPU_request_unbuffered > hw_upper:
                    hw_CPU_request = hw_target + rescale_buffer
                    hw_cooldown = rescale_cooldown
            # elseIf under lower
                elif hw_CPU_request_unbuffered < hw_lower:
                    hw_CPU_request = hw_target + rescale_buffer
                    hw_cooldown = rescale_cooldown

        # Reduce cooldown
        if hw_cooldown > 0:
            hw_cooldown -= 1

        hw_requests.append(hw_CPU_request)

        # LSTM scaling
        lstm_CPU_request_unbuffered = lstm_CPU_request - rescale_buffer

        # If no cool-down
        if (lstm_cooldown == 0):
            # If request change greater than 50
            if (abs(lstm_CPU_request - (lstm_target + rescale_buffer)) > 50):
                # If above upper
                if lstm_CPU_request_unbuffered > lstm_upper:
                    lstm_CPU_request = lstm_target + rescale_buffer
                    lstm_cooldown = rescale_cooldown
                # elseIf under lower
                elif lstm_CPU_request_unbuffered < lstm_lower:
                    lstm_CPU_request = lstm_target + rescale_buffer
                    lstm_cooldown = rescale_cooldown

        # Reduce cooldown
        if lstm_cooldown > 0:
            lstm_cooldown -= 1

        lstm_requests.append(lstm_CPU_request)

        i += 1
    return hw_upper, hw_lower, hw_target, lstm_upper, lstm_lower, lstm_target

