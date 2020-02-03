import random

class Predict:
    def __init__(self, json_str):
        self.params = json_str

    def turnover(self):
        params = self.params
        params_length = len(params)

        if params_length > 0:  # 获得参数
            result = [['2019-07-21', random.randint(0, 99), '晴'],
                      ['2019-07-22', random.randint(0, 99), '阴'],
                      ['2019-07-23', random.randint(0, 99), '雨']]
        else:  # 未获得参数
            result = [['hello world'], ['hello python']]

        # print(result)
        return result