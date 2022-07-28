class MathSa():
    def __init__(self):
        pass

    # 最大值
    def get_max(self, data: list):
        return max(data)

    # 最小值
    def get_min(self, data: list):
        return min(data)

    # 极差
    def get_range(self, data: list):
        return max(data) - min(data)

    # 中位数
    def get_median(self, data: list, save_point=2):
        data = sorted(data)
        size = len(data)
        if size % 2 == 0:  # 判断列表长度为偶数
            median = round((data[size // 2] + data[size // 2 - 1]) / 2, save_point)
        if size % 2 == 1:  # 判断列表长度为奇数
            median = data[(size - 1) // 2]
        return median

    # 众数(返回多个众数的平均值)
    def get_most(self, data: list, save_point=2):
        most = []
        item_num = dict((item, data.count(item)) for item in data)
        for k, v in item_num.items():
            if v == max(item_num.values()):
                most.append(k)
        return round(sum(most) / len(most), save_point)

    # 获取平均数/期望
    def get_average(self, data: list, save_point=2):
        sum = 0
        for item in data:
            sum += item
        return round(sum / len(data), save_point)

    # 获取方差
    def get_variance(self, data: list, save_point=2):
        sum = 0
        average = self.get_average(data)
        for item in data:
            sum += (item - average) ** 2
        return round(sum / len(data), save_point)

    # 获取n阶原点距
    def get_nmoment(self, data: list, n=2, save_point=2):
        sum = 0
        for item in data:
            sum += item ** n
        return round(sum / len(data), save_point)

    def math_aggregation(self, data: list, save_point=2):
        math_sa = {
            'max': self.get_max(data),
            'min': self.get_min(data),
            'range': self.get_range(data),
            'median': self.get_median(data, save_point),
            'most': self.get_most(data, save_point),
            'average': self.get_average(data, save_point),
            'variance': self.get_variance(data, save_point),
            'nmoment': self.get_nmoment(data, 2, save_point)
        }
        return math_sa
