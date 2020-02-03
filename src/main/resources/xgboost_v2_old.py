#coding=utf-8
import sys
import datetime
import re
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb  # 原生xgboost
# from sklearn.metrics import mean_squared_error  # 计算MSE评价指标
# from sklearn.metrics import mean_absolute_error  # 计算MAE评价指标
# from sklearn.metrics import r2_score  # 计算R2评价指标
# from sklearn.cross_validation import cross_val_score  # 交叉验证
# from xgboost import plot_importance


'''
Update Date: 2019-08-16
Author: Wang Yongxiang
-----本次更新-----
1.用门店名称替换门店编号作为门店筛选条件
2.预测天数可选7或15天
3.增加历史数据可选全部
4.加入了对缺失值的处理
5.优化了数据处理过程
6.最终输出结果的天气用数字表示

-----下次改进-----
1.使用最新的天气数据
2.增加季节、月份的操作在最后组合数据的时候再进行，较少因缺失数据浪费的计算
'''
class Predict:
    def __init__(self, json_str, conn):
        self.params = json_str
        self.city = self.params['city']  # 城市，查天气用
        self.groupCode = self.params['group_code']  # 集团code
        self.storeName = self.params['store_name']  # 门店code
        self.historyDate = self.params['history_date'].split(',')  # 门店历史数据的起止时间
        self.predictDays = self.params['predict_days']
        self.allData = self.params['all_data']
        self.connection = conn  # impala地址

    # 获取天气预报数据
    def query_weather_predict(self):
        with self.connection.cursor() as cur:
            # 预测时间段的起止日期
            today_date = get_n_after_days(self.predictDays)[0]
            end_date = get_n_after_days(self.predictDays)[1]

            # 拼接sql语句，
            weather_sql = "SELECT day," \
                          "m_weather," \
                          "n_weather," \
                          "m_temper," \
                          "n_temper," \
                          "m_wind " \
                          "FROM zhaoyaxing.weather " \
                          "WHERE city LIKE {cityName} " \
                          "AND (day BETWEEN {startDate} AND {endDate}) " \
                          "ORDER BY day ASC" \
                .format(cityName="'" + self.city + "%'",
                        startDate="'" + today_date + "'",
                        endDate="'" + end_date + "'")

            # 查询impala
            cur.execute(weather_sql)

            # 按照日期对数据进行去重
            df = as_pandas(cur).drop_duplicates(subset=['day']).reset_index(drop=True)
            df = df.dropna(axis=0, how='any').reset_index(drop=True)

            # 获取数据条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                print('天气预报数据缺失')
                print('')
                sys.exit(0)
            else:
                # 转换日期格式
                new_date = list(map(date_transfer, df['day']))
                df['day'] = new_date

                # 转换风力格式
                new_windforce_m = list(map(windforce_transfer, df['m_wind']))
                df['m_wind'] = new_windforce_m

                # 转换天气格式
                new_weather_n = list(map(weather_transfer, df['n_weather']))
                df['n_weather'] = new_weather_n
                new_weather_m = list(map(weather_transfer, df['m_weather']))
                df['m_weather'] = new_weather_m

                # 将温度转换为int型
                df['n_temper'] = df['n_temper'].astype(int)
                df['m_temper'] = df['m_temper'].astype(int)

                # 修改日期列名和日期数据一致
                df.rename(columns={'day': 'settle_biz_date'}, inplace=True)

                return df

    # 获取营业额训练数据
    def query_date(self):
        with self.connection.cursor() as cur:
            # 获取groupCode的拼接字符串
            group_str = split_groupCodes_sql(self.groupCode)

            # 获取storeCode的拼接字符串
            store_str = split_storeNames_sql(self.storeName)

            if self.allData == 0:  # 选择部分历史数据
                # 拼接sql语句
                store_sql = "SELECT settle_biz_date, " \
                                   "settle_biz_week, " \
                                   "sum(recv_money) as sum_recv_money " \
                            "FROM e000.dw_trade_bill_fact_p_group_upgrade " \
                            "WHERE  (CAST(group_code as string) REGEXP {groupStr}) ".format(groupStr=group_str) + \
                                  "AND (CAST(store_name as string) REGEXP {storeStr}) ".format(storeStr=store_str) + \
                                  "AND (settle_biz_date BETWEEN {startDate} AND {endDate}) " \
                                      .format(startDate="'"+self.historyDate[0]+"'", endDate="'"+self.historyDate[1]+"'") + \
                            "GROUP BY settle_biz_date,settle_biz_week " \
                            "ORDER BY settle_biz_date ASC"

            elif self.allData == 1:  # 选择全部历史数据
                # 昨天的日期
                yesterday_date = getBeforeDate(1)

                # 2年前的日期
                before_date = getBeforeDate(730)

                # 拼接sql语句
                store_sql = "SELECT settle_biz_date, " \
                                   "settle_biz_week, " \
                                   "sum(recv_money) as sum_recv_money " \
                            "FROM e000.dw_trade_bill_fact_p_group_upgrade " \
                            "WHERE  (CAST(group_code as string) REGEXP {groupStr}) ".format(groupStr=group_str) + \
                                  "AND (CAST(store_name as string) REGEXP {storeStr}) ".format(storeStr=store_str) + \
                                  "AND (settle_biz_date BETWEEN {startDate} AND {endDate}) " \
                                      .format(startDate="'"+before_date+"'", endDate="'"+yesterday_date+"'") + \
                            "GROUP BY settle_biz_date,settle_biz_week " \
                            "ORDER BY settle_biz_date ASC"

            # 查询impala
            cur.execute(store_sql)

            # 将查询结果转换成DataFrame格式
            df = as_pandas(cur)
            df = df.dropna(axis=0, how='any').reset_index(drop=True)

            # 获取数据条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                print('营业额历史数据缺失')
                print('')
                sys.exit(0)
            else:
                # 星期几转换成数字，并更新DataFrame
                weekdays = list(map(weekday_transfer, df['settle_biz_week']))
                df['settle_biz_week'] = weekdays

                # 获取月份
                months = list(map(month_transfer, df['settle_biz_date']))
                df.insert(2, 'month', months)

                # 获取季节
                seasons = list(map(season_transfer, df['month']))
                df.insert(3, 'season', seasons)

                return df

    # 获取天气历史数据
    def query_weather_train(self):
        with self.connection.cursor() as cur:
            if self.allData == 0:  # 选择部分历史数据
                # 拼接sql语句，
                weather_sql = "SELECT day," \
                                     "m_weather," \
                                     "n_weather," \
                                     "m_temper," \
                                     "n_temper," \
                                     "m_wind " \
                              "FROM zhaoyaxing.weather " \
                              "WHERE city LIKE {cityName} " \
                                    "AND (day BETWEEN {startDate} AND {endDate}) " \
                              "ORDER BY day ASC" \
                    .format(cityName="'" + self.city + "%'",
                            startDate="'" + self.historyDate[0] + "'",
                            endDate="'" + self.historyDate[1] + "'")

            elif self.allData == 1:  # 选择全部历史数据
                # 昨天的日期
                yesterday_date = getBeforeDate(1)

                # 2年前的日期
                before_date = getBeforeDate(730)

                # 拼接sql语句，
                weather_sql = "SELECT day," \
                                     "m_weather," \
                                     "n_weather," \
                                     "m_temper," \
                                     "n_temper," \
                                     "m_wind " \
                              "FROM zhaoyaxing.weather " \
                              "WHERE city LIKE {cityName} " \
                                    "AND (day BETWEEN {startDate} AND {endDate}) " \
                              "ORDER BY day ASC" \
                    .format(cityName="'" + self.city + "%'",
                            startDate="'" + before_date + "'",
                            endDate="'" + yesterday_date + "'")

            # 查询impala
            cur.execute(weather_sql)

            # 按照日期对数据进行去重
            df = as_pandas(cur).drop_duplicates(subset=['day']).reset_index(drop=True)
            df = df.dropna(axis=0, how='any').reset_index(drop=True)

            # 获取数据的条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                print('天气历史数据缺失')
                print('')
                sys.exit(0)
            else:
                # 转换日期格式
                new_date = list(map(date_transfer, df['day']))
                df['day'] = new_date

                # 转换风力格式
                new_windforce_m = list(map(windforce_transfer, df['m_wind']))
                df['m_wind'] = new_windforce_m

                # 转换天气格式
                new_weather_n = list(map(weather_transfer, df['n_weather']))
                df['n_weather'] = new_weather_n
                new_weather_m = list(map(weather_transfer, df['m_weather']))
                df['m_weather'] = new_weather_m

                # 将温度转换为int型
                df['n_temper'] = df['n_temper'].astype(int)
                df['m_temper'] = df['m_temper'].astype(int)

                # 修改日期列名和日期数据一致
                df.rename(columns={'day': 'settle_biz_date'}, inplace=True)

                return df

# 组合训练数据
def combine_train_data(weather_train, date_train, weather_predict):
    # 将日期数据和天气数据合并生成训练数据，拼接两个DataFrame
    train_data = pd.merge(weather_train, date_train, how='left', on='settle_biz_date')
    train_data = train_data.dropna(axis=0, how='any').reset_index(drop=True)

    lines_num = train_data.iloc[:, 0].size  # 数据量

    # 判断训练数据量是否大于15
    if lines_num == 0:
        print('训练数据缺失')
        print('')
        sys.exit(0)
    elif 0 < lines_num < 15:
        train_features_reserved = train_data
    else:
        # 预留15条数据，组合最终结果用
        train_features_reserved = train_data.tail(15)

    # 训练预留数据删除无用列
    del train_features_reserved['settle_biz_week']
    del train_features_reserved['month']
    del train_features_reserved['season']

    # 训练标签数据
    train_label = train_data['sum_recv_money']

    # 移除无用列，生成特征训练数据
    del train_data['settle_biz_date']
    del train_data['sum_recv_money']
    train_features = train_data
    # 给特征训练数据添加weekend列
    train_features["weekend"] = train_features.apply(lambda x: isWeekend(x["settle_biz_week"]), axis=1)
    # 给特征训练数据添加节日列
    train_features['festival'] = train_features.apply(lambda x: isFestival(x["settle_biz_week"]), axis=1)

    # 给预测数据添加星期列
    add_week = list(map(date_to_weekday_transfer, weather_predict['settle_biz_date']))
    weather_predict['settle_biz_week'] = add_week

    # 给预测数据添加月份列
    add_month = list(map(month_transfer, weather_predict['settle_biz_date']))
    weather_predict['month'] = add_month

    # 给预测数据添加季节列
    add_season = list(map(season_transfer, weather_predict['month']))
    weather_predict['season'] = add_season

    # 生成特征预测数据
    predict_features = weather_predict
    # 给特征预测数据添加weekend列
    predict_features["weekend"] = predict_features.apply(lambda x: isWeekend(x["settle_biz_week"]), axis=1)
    # 给特征预测数据添加节日列
    predict_features['festival'] = predict_features.apply(lambda x: isFestival(x["settle_biz_week"]), axis=1)

    return train_features, train_label, predict_features, train_features_reserved

# xgboost模型
def xgboost_model(train_features, train_label, predict_features):
    # 预测数据的条数
    predict_features_num = predict_features.iloc[:, 0].size

    # 获取预留的天气预测数据，最后汇总结果用
    predict_features_reserved = predict_features.copy()

    # 删除日期列
    del predict_features['settle_biz_date']

    # 将训练数据和预测数据上下拼接在一起
    all_data = pd.concat([train_features, predict_features], axis=0).reset_index(drop=True)

    # 对全部数据的特征进行one-hot处理
    all_data_ontHot = pd.get_dummies(all_data, columns=['settle_biz_week', 'month', 'season',
                                                        'm_weather', 'n_weather',
                                                        'm_wind', 'weekend', 'festival'])

    # 预测数据one_hot
    predict_features_oneHot = all_data_ontHot.tail(predict_features_num).reset_index(drop=True)

    # 训练数据one_hot
    train_features_oneHot = all_data_ontHot.drop(list(all_data.tail(predict_features_num).index.values))

    # 将数据划分为训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(train_features_oneHot, train_label,
                                                        test_size=0.3, random_state=123)

    # 模型参数设置
    params = {
        'booster': 'gbtree',  # 迭代模型，还有gbliner
        'objective': 'reg:squarederror',  # gamma, logistic, linear, squarederror
        'eval_metric': 'rmse',  # 损失函数，rmse或mae
        'max_depth': 4,  # 树的最大深度（一般取值3~10），降低模型复杂度，防止过拟合。取值范围 1-无穷
        'min_child_weight': 10,  # 最小样本权重的和。防止过拟合，过大则欠拟合。取值范围 0-无穷
        # 'gamma': 0.1,  # 节点分裂所需损失函数下降的最小值，大于此值才分裂，防止过拟合，控制后剪枝。取值范围 0-无穷。（没有效果？可能是max_depth的作用）
        'subsample': 0.6,  # 行采样，每棵树随机采样数据的比例，(一般取值0.5-1)，增加随机性。取值范围 0-1
        'colsample_bytree': 0.6,  # 列采样，每棵树随机采样特征的比例，(一般取值0.5-1)，增加随机性。取值范围 0-1
        'alpha': 0,  # L1正则化项系数，一般为0
        'lambda': 3,  # L2正则化项系数
        'eta': 0.1,  # 学习率，一般取值范围：0.01-0.2
        'scale_pos_weight': 1,  # 默认值为1，各类别样本十分不均衡时该参数设为正值，可使算法更快收敛
        'silent': 1,  # 是否输出训练过程
        # 'early_stopping_rounds': True,  # 结果没有提升就退出迭代
        # 'verbose_eval': 10,  # 输出评估信息，设为5则每5次评估输出一次.
        # 'seed': 123  # 种子
    }

    # 准备训练数据
    dtrain = xgb.DMatrix(x_train, y_train)  # 训练集

    # dtest = xgb.DMatrix(x_test, y_test)  # 测试集
    # watch_list = [(dtrain, 'train'), (dtest, 'test')]  # 训练过程的显示内容

    # 交叉验证
    cv_result = xgb.cv(params,
                       dtrain,
                       nfold=10,
                       num_boost_round=200,
                       early_stopping_rounds=10,
                       # verbose_eval=10,
                       metrics='rmse',
                       show_stdv=False)
    '''
    cv_res.shape[0]为最佳迭代次数
    print('最佳迭代次数：%d' % cv_result.shape[0])
    定义模型。迭代 num_boost_round 次
    '''
    # print('最佳迭代次数：%d' % cv_result.shape[0])

    # model = xgb.train(params=params, dtrain=dtrain, num_boost_round=cv_result.shape[0], evals=watch_list, verbose_eval=10)
    model = xgb.train(params=params, dtrain=dtrain, num_boost_round=cv_result.shape[0])

    # 模型输出结果，使用predict_features_oneHot作为预测模型的输入数据
    result = model.predict(xgb.DMatrix(predict_features_oneHot), ntree_limit=model.best_iteration)

    # # 计算评估指标
    # # RMSE
    # mse = mean_squared_error(y_test, result)
    # rmse = mse ** 0.5
    # print('RMSE: %d' % int(rmse))
    # # MAE
    # mae = mean_absolute_error(y_test, result)
    # print('MAE: %d' % int(mae))
    # # R2
    # r2 = r2_score(y_test, result)
    # print('R2: %f' % r2)
    #
    # # 输出特征重要性排行
    # plot_importance(model)

    # 将list中的元素转换成整数
    result = [int(i) for i in result.tolist()]

    # 给预测预留数据增加结果列，返回最终的预测结果
    predict_features_reserved['sum_recv_money'] = result  # 预测结果生成DataFrame

    return predict_features_reserved

# 将历史预留数据和预测数据拼接成最终结果
def get_final_result(train_features_reserved, predict_features_reserved):
    #拼接最终结果
    result_df = pd.concat([train_features_reserved, predict_features_reserved], ignore_index=True, sort=True)

    result_list = []  # 需要返回的最终结果
    # 将最终结果组成 dict-in-list形式
    for index, row in result_df.iterrows():
        date_day = row['settle_biz_date']
        m_weather = row['m_weather']  # 将天气label转换成文字
        n_weather = row['n_weather']
        m_temper = row['m_temper']
        n_temper = row['n_temper']
        m_wind = row['m_wind']
        sum_recv_money = row['sum_recv_money']

        # 将结果组成JSON
        dic = {'date': date_day, 'sum_recv_money': sum_recv_money,
               'm_weather': m_weather, 'n_weather': n_weather,
               'm_temper': m_temper, 'n_temper': n_temper,
               'm_wind': m_wind}

        result_list.append(dic)

    return result_list

# ---------------------------------转换函数---------------------------------------

# 拼接参数storeCode的sql语句片段
def split_groupCodes_sql(codes_str):
    if ',' in codes_str:
        # 多个门店号的情况
        codes = codes_str.split(',')
        group_list = []
        for i in range(len(codes)):
            group_list.append(int(codes[i]))
        group_list_split = "|".join(str(i) for i in group_list)
        group_str = "'" + group_list_split + "'"
    else:
        # 只有一个门店号的情况
        group_str = "'" + codes_str + "'"
    return group_str

def split_storeNames_sql(names_str):
    if ',' in names_str:
        # 多个门店号的情况
        names = names_str.split(',')
        store_list = []
        for i in range(len(names)):
            store_list.append(names[i])
        store_list_split = "|".join(i for i in store_list)
        store_str = "'" + store_list_split + "'"
    else:
        # 只有一个门店号的情况
        store_str = "'" + names_str + "'"
    return store_str

# 将星期几转换成数字
def weekday_transfer(weekday):
    day_of_week = {'周一': 1,
                   '周二': 2,
                   '周三': 3,
                   '周四': 4,
                   '周五': 5,
                   '周六': 6,
                   '周日': 7}
    return day_of_week[weekday]

# 根据日期获取月份
def month_transfer(date_str):
    new_month = str(int(date_str.split('-')[1]))
    return new_month

# 将月份转换成季节数字标签，春[1]:3,4,5月；夏[2]:6,7,8月；秋[3]:9,10,11月；冬[4]:12,1,2月
def season_transfer(month_str):
    month_season = {'12': 4, '1': 4, '2': 4,
                    '3': 1, '4': 1, '5': 1,
                    '6': 2, '7': 2, '8': 2,
                    '9': 3, '10': 3, '11': 3}
    season = month_season[month_str]
    return season

# 将2019年7月16日转换成2019-07-16格式
def date_transfer(date_str):
    temp_date = date_str.replace(r'年', '-').replace(r'月', '-').replace(r'日', '')
    new_date = str(datetime.datetime.strptime(temp_date, '%Y-%m-%d')).split(' ')[0]
    return new_date

# 将日期转换成周几数字标签
def date_to_weekday_transfer(date_str):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    week_day_dict = {
        0: 1,
        1: 2,
        2: 3,
        3: 4,
        4: 5,
        5: 6,
        6: 7,
    }
    day = date.weekday()
    weekday = week_day_dict[day]  # 周几的数字标签
    return weekday

# 获取今天日期、7天后的日期(包含今天)、未来7天的日期列表
def get_n_after_days(predict_days):
    day_today = '2018-12-16'  # 今天的日期，2019-08-13
    # day_today = str(datetime.date.today())  # 今天的日期，2019-08-13
    # now = datetime.datetime.now()  # 当前日期时间，2019-08-13 16:45:14.787010
    now_str='2018-12-16 16:45:14'
    now = datetime.datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S')
    delta = datetime.timedelta(days=(predict_days-1))  # 6或14
    n_days_after = (now + delta).strftime('%Y-%m-%d')  # 从今天开始第七天的日期
    after_dates = []  # 未来7天的日期列表
    temp_date = datetime.datetime.strptime(day_today, "%Y-%m-%d")
    date = str(day_today)[:]
    while date <= n_days_after:
        after_dates.append(date)
        temp_date = temp_date + datetime.timedelta(1)
        date = temp_date.strftime("%Y-%m-%d")
    return day_today, n_days_after, after_dates

# 将'<3级'风力转换成数字格式
def windforce_transfer(windforce_str):
    if windforce_str == '-':
        windforce = 'Null'
    elif windforce_str == '无持续风向微风':
        windforce = 0
    else:
        windforce = re.findall(r"\d", windforce_str)[0]
    return windforce

'''
转换天气格式，将天气转换成数字标签
晴:1，阴:2，多云:3，刮风:4，沙尘:5，雾霾:6，
小雨:7，中雨:8，大雨:9，
小雪:10，中雪:11，大雪:12，'-':Null
'''
def weather_transfer(weather_str):
    if '晴' in weather_str:
        weather = 1
    elif '阴' in weather_str:
        weather = 2
    elif '云' in weather_str:
        weather = 3
    elif '风' in weather_str:
        weather = 4
    elif '沙' in weather_str \
            or '尘' in weather_str:
        weather = 5
    elif '雾' in weather_str \
            or '霾' in weather_str:
        weather = 6
    elif weather_str == '毛毛雨' \
            or weather_str == '细雨' \
            or weather_str == '零散雷雨' \
            or weather_str == '小雨' \
            or weather_str == '零散阵雨':
        weather = 7
    elif weather_str == '小到中雨' \
            or weather_str == '雷雨' \
            or weather_str == '阵雨' \
            or weather_str == '雷阵雨' \
            or weather_str == '雨' \
            or weather_str == '中雨' \
            or weather_str == '小雨-中雨':
        weather = 8
    elif weather_str == '大雨' \
            or weather_str == '冻雨' \
            or weather_str == '中雨-大雨' \
            or weather_str == '暴雨' \
            or weather_str == '中到大雨' \
            or weather_str == '大雨-暴雨' \
            or weather_str == '雨夹雪':
        weather = 9
    elif weather_str == '阵雪' \
            or weather_str == '零散阵雪' \
            or weather_str == '小雪':
        weather = 10
    elif weather_str == '雪' \
            or weather_str == '小雪-中雪' \
            or weather_str == '中雪' \
            or weather_str == '小到中雪':
        weather = 11
    elif weather_str == '大雪-暴雪' \
            or weather_str == '暴雪' \
            or weather_str == '中雪-大雪' \
            or weather_str == '中到大雪' \
            or weather_str == '大雪':
        weather = 12
    elif weather_str == '-':
        weather = 0
    return weather

# 将天气数据标签转换成文字
def label_weather_transfer(label):
    label_dict = {1: '晴', 2: '阴', 3: '多云', 4: '有风', 5: '沙尘', 6: '雾霾',
                  7: '小雨', 8: '中雨', 9: '大雨',
                  10: '小雪', 11: '中雪', 12: '大雪', 0: '-'}
    return label_dict[label]

# 判断一天是否是周末
def isWeekend(weekday):
    if weekday == 6 or weekday == 7:
        return 1
    else:
        return 0

# 获取N天前的日期
def getBeforeDate(beforeDays):
    today = datetime.datetime.now()
    # 计算偏移量
    offset = datetime.timedelta(days=-beforeDays)
    # 获取想要的日期的时间
    before_date = (today + offset).strftime('%Y-%m-%d')
    return before_date

# 判断一天是否是节日 TODO 搜集节日
def isFestival(date_str):
    festival_days = ['2018-01-01', '2018-02-14', '2018-02-15', '2018-02-16', '2018-02-17',
                     '2018-02-18', '2018-02-19', '2018-02-20', '2018-02-21', '2018-03-02',
                     '2018-03-08', '2018-04-05', '2018-04-06', '2018-04-07', '2018-04-29',
                     '2018-04-30', '2018-05-01', '2018-06-01', '2018-06-16', '2018-06-17',
                     '2018-06-18', '2018-08-17', '2018-09-22', '2018-09-23', '2018-09-24',
                     '2018-10-01', '2018-10-02', '2018-10-03', '2018-10-04', '2018-10-05',
                     '2018-10-06', '2018-10-07', '2018-11-22', '2018-12-24', '2018-12-25']
    if date_str in festival_days:
        return 1
    else:
        return 0

# # 获取传递的参数
# def get_params(params_str):
#     city = params_str[1]
#     group_code = params_str[2]
#     store_code = params_str[3]
#     history_date = params_str[4]
#     # 将参数组成字典
#     params = {'city': city,
#               'group_code': group_code,
#               'store_code': store_code,
#               'history_date': history_date}
#     return params

if __name__ == '__main__':

    try:

        params = eval(sys.argv[1])

        # params = {'city': '天津', 'group_code': '3297',
        # 'store_name': '九田家天津武清区体育中心店,九田家天津和平区吉利商厦店,九田家天津红桥区欧亚达,九田家天津南开区熙悦汇店,九田家天津武清区保利金街店,九田家天津滨海新区八角楼店',
        # 'history_date': '2018-07-01,2018-12-31',
        # 'predict_days': 15, 'all_data': 0}

        conn = connect(host='192.168.12.204', port=21050)

        weather_predict = Predict(params, conn).query_weather_predict()  # 天气预测数据

        date_train = Predict(params, conn).query_date()  # 营业额训练数据

        weather_train = Predict(params, conn).query_weather_train()  # 天气训练数据

        # 特征训练数据、特征标签、特征预测数据、训练数据预留
        train_features, train_label, predict_features, train_features_reserved = combine_train_data(weather_train, date_train, weather_predict)

        predict_result = xgboost_model(train_features, train_label, predict_features)  # 预测结果

        result = get_final_result(train_features_reserved, predict_result)  # 返回最终结果
        print(result)
        print(len(result))

        conn.close()  # 关闭连接

    finally:
        conn.close()
