import utils
class SimpleScaler:
    def __init__(self):
        print("Initting")
        self.cpu_util_history = []
        self.throttled_percent_history = []
        self.quota_history = []
        self.period_history = []

    def autotune(self, containers):
        self.learn(containers)
        self.recommend(containers)

    def learn(containers):
        print("Learning")
        utils.get_util_info(containers)
        print(containers)

    def recommend(containers):
        print("Recommending")
