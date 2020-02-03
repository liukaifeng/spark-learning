# coding=utf-8
import os
import logging
from logging import handlers
import sys
import json
import demjson
import datetime
import re
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb  # 原生xgboost
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 日志类
class Logger(object):
    # 日志级别关系映射
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    def __init__(self, filename, level='info', when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)  # 设置日志格式
        self.logger.setLevel(self.level_relations.get(level))  # 设置日志级别
        # sh = logging.StreamHandler()#往屏幕上输出
        # sh.setFormatter(format_str) #设置屏幕上显示的格式
        # 往文件里写入指定间隔时间自动生成文件的处理器
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount, encoding='utf-8')
        th.setFormatter(format_str)  # 设置文件里写入的格式
        # self.logger.addHandler(sh) #把对象加到logger里
        self.logger.addHandler(th)

# 预测类
class Predict:
    def __init__(self, params_dict, conn):
        self.city = params_dict['city']  # 城市，查天气用
        self.groupCode = params_dict['group_code']  # 集团code
        self.storeName = params_dict['store_name']  # 门店code#
        self.historyDate = params_dict['history_date'].split(',')  # 门店历史数据的起止时间
        self.predictDays = params_dict['predict_days']
        self.allData = params_dict['all_data']
        self.connection = conn  # impala地址
        self.weatherTable = 'worm.weather'  # 天气数据表
        self.salesTable = 'e000.dw_trade_bill_fact_p_group_upgrade'  # 营业额数据表

    # 获取天气预报数据
    def query_weather_predict(self):
        log.logger.info('Start query future weather data')
        with self.connection.cursor() as cur:
            # 预测时间段的起止日期
            today_date = get_n_after_days(self.predictDays)[0]
            end_date = get_n_after_days(self.predictDays)[1]
            # 拼接sql语句，
            weather_sql = "SELECT recorddate," \
                          "dayweather," \
                          "nightweather," \
                          "daytemperature," \
                          "nighttemperature," \
                          "daywindforce " \
                          "FROM " \
                          "{weather_table} ".format(weather_table=self.weatherTable) + \
                          "WHERE " \
                          "city LIKE CONCAT(REPLACE({cityName},'市','%'),'%') " \
                          "AND (CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) BETWEEN {startDate} AND {endDate}) " \
                          "ORDER BY " \
                          "CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) ASC" \
                              .format(cityName="'" + self.city + "%'",
                                      startDate="'" + today_date + "'",
                                      endDate="'" + end_date + "'")

            log.logger.info('SQL-1: ' + weather_sql)
            # 查询impala
            cur.execute(weather_sql)
            # 按照日期对数据进行去重
            df = as_pandas(cur).drop_duplicates(subset=['recorddate']).reset_index(drop=True)
            # 空值处理
            df = df.dropna(axis=0, how='any').reset_index(drop=True)
            # 获取数据条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                log.logger.info('No matched future weather data')
                print('')
                sys.exit(0)
            else:
                # 转换日期格式
                df['recorddate'] = [date_transfer(i) for i in df['recorddate'].tolist()]
                # 转换风力格式
                df['daywindforce'] = [windforce_transfer(i) for i in df['daywindforce'].tolist()]
                # 转换天气格式
                df['dayweather'] = [weather_transfer(i) for i in df['dayweather'].tolist()]
                df['nightweather'] = [weather_transfer(i) for i in df['nightweather'].tolist()]
                # 转换温度格式
                df['daytemperature'] = [temperature_transfer(i) for i in df['daytemperature'].tolist()]
                df['nighttemperature'] = [temperature_transfer(i) for i in df['nighttemperature'].tolist()]
                # 修改日期列名和日期数据一致
                df.rename(columns={'recorddate': 'settle_biz_date'}, inplace=True)
                log.logger.info('Future weather dataframe ready')
                return df

    # 获取营业额训练数据
    def query_date(self):
        log.logger.info('Start query sales data')
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
                            "FROM " \
                            "{sales_table} ".format(sales_table=self.salesTable) + \
                            "WHERE (CAST(group_code as string) REGEXP {groupStr}) ".format(groupStr=group_str) + \
                            "AND (CAST(store_name as string) REGEXP {storeStr}) ".format(storeStr=store_str) + \
                            "AND (settle_biz_date BETWEEN {startDate} AND {endDate}) " \
                                .format(startDate="'" + self.historyDate[0] + "'",
                                        endDate="'" + self.historyDate[1] + "'") + \
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
                            "FROM " \
                            "{sales_table} ".format(sales_table=self.salesTable) + \
                            "WHERE  (CAST(group_code as string) REGEXP {groupStr}) ".format(groupStr=group_str) + \
                            "AND (CAST(store_name as string) REGEXP {storeStr}) ".format(storeStr=store_str) + \
                            "AND (settle_biz_date BETWEEN {startDate} AND {endDate}) " \
                                .format(startDate="'" + before_date + "'", endDate="'" + yesterday_date + "'") + \
                            "GROUP BY settle_biz_date,settle_biz_week " \
                            "ORDER BY settle_biz_date ASC"

            log.logger.info('SQL-2: ' + store_sql)
            # 查询impala
            cur.execute(store_sql)
            # 将查询结果转换成DataFrame格式
            df = as_pandas(cur)
            # 空值处理
            df = df.dropna(axis=0, how='any').reset_index(drop=True)
            # 获取数据条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                log.logger.info('No matched sales data')
                print('')
                sys.exit(0)
            else:
                # 星期几转换成数字，并更新DataFrame
                df['settle_biz_week'] = [weekday_transfer(i) for i in df['settle_biz_week'].tolist()]
                # 获取月份
                months = [month_transfer(i) for i in df['settle_biz_date'].tolist()]
                df.insert(2, 'month', months)
                # 获取季节
                seasons = [season_transfer(i) for i in df['month'].tolist()]
                df.insert(3, 'season', seasons)
                log.logger.info('Sales dataframe ready')
                return df

    # 获取天气历史数据
    def query_weather_train(self):
        log.logger.info('Start query history weather data')
        with self.connection.cursor() as cur:
            # 判断是否使用全部数据
            if self.allData == 0:  # 选择部分历史数据
                start_date = self.historyDate[0]
                end_date = self.historyDate[1]
                # 拼接sql语句，
                weather_sql = "SELECT recorddate," \
                              "dayweather," \
                              "nightweather," \
                              "daytemperature," \
                              "nighttemperature," \
                              "daywindforce " \
                              "FROM " \
                              "{weather_table} ".format(weather_table=self.weatherTable) + \
                              "WHERE " \
                              "city LIKE CONCAT(REPLACE({cityName},'市','%'),'%') " \
                              "AND (CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) BETWEEN {startDate} AND {endDate}) " \
                              "ORDER BY " \
                              "CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) ASC" \
                                  .format(cityName="'" + self.city + "%'",
                                          startDate="'" + start_date + "'",
                                          endDate="'" + end_date + "'")

            elif self.allData == 1:  # 选择全部历史数据
                # 昨天的日期
                yesterday_date = date_transfer_reverse(getBeforeDate(1))
                # 2年前的日期
                before_date = date_transfer_reverse(getBeforeDate(730))
                # 拼接sql语句，
                weather_sql = "SELECT recorddate," \
                              "dayweather," \
                              "nightweather," \
                              "daytemperature," \
                              "nighttemperature," \
                              "daywindforce " \
                              "FROM " \
                              "{weather_table} ".format(weather_table=self.weatherTable) + \
                              "WHERE " \
                              "city LIKE CONCAT(REPLACE({cityName},'市','%'),'%') " \
                              "AND (CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) BETWEEN {startDate} AND {endDate}) " \
                              "ORDER BY " \
                              "CAST(REPLACE(REPLACE(replace(recorddate, '年', '-'), '月', '-'), '日', '') as TIMESTAMP) ASC" \
                                  .format(cityName="'" + self.city + "%'",
                                          startDate="'" + before_date + "'",
                                          endDate="'" + yesterday_date + "'")

            log.logger.info('SQL-3: ' + weather_sql)
            # 查询impala
            cur.execute(weather_sql)
            # 按照日期对数据进行去重
            df = as_pandas(cur).drop_duplicates(subset=['recorddate']).reset_index(drop=True)
            # 空值处理
            df = df.dropna(axis=0, how='any').reset_index(drop=True)
            # 获取数据的条数
            lines_num = df.iloc[:, 0].size
            if lines_num == 0:
                log.logger.info('No matched history weather data')
                print('')
                sys.exit(0)
            else:
                # 转换日期格式
                df['recorddate'] = [date_transfer(i) for i in df['recorddate'].tolist()]
                # 修改日期列名和日期数据一致
                df.rename(columns={'recorddate': 'settle_biz_date'}, inplace=True)
                log.logger.info('History weather dataframe ready')
                return df

# 组合训练数据
def combine_train_data(weather_train, date_train, weather_predict):
    log.logger.info('Start combine train data and predict data')
    # 将日期数据和天气数据合并生成训练数据，拼接两个DataFrame
    train_data = pd.merge(weather_train, date_train, how='left', on='settle_biz_date')
    # 空值处理
    train_data = train_data.dropna(axis=0, how='any').reset_index(drop=True)
    # 转换风力格式
    train_data['daywindforce'] = [windforce_transfer(i) for i in train_data['daywindforce'].tolist()]
    # 转换天气格式
    train_data['dayweather'] = [weather_transfer(i) for i in train_data['dayweather'].tolist()]
    train_data['nightweather'] = [weather_transfer(i) for i in train_data['nightweather'].tolist()]
    # 转换温度格式
    train_data['daytemperature'] = [temperature_transfer(i) for i in train_data['daytemperature'].tolist()]
    train_data['nighttemperature'] = [temperature_transfer(i) for i in train_data['nighttemperature'].tolist()]

    lines_num = train_data.iloc[:, 0].size  # 统计数据量
    # 判断训练数据量是否大于15
    if lines_num == 0:
        log.logger.info('No matched combined train data')
        print('')
        sys.exit(0)
    elif 0 < lines_num < 15:
        log.logger.info('Number of train features data is: ' + str(lines_num))
        train_features_reserved = train_data.copy()
    else:
        # 预留15条数据，组合最终结果用
        log.logger.info('Number of train features data is: ' + str(lines_num))
        temp_df = train_data.copy()
        train_features_reserved = temp_df.tail(15)

    # 训练预留数据删除无用列
    del train_features_reserved['settle_biz_week']
    del train_features_reserved['month']
    del train_features_reserved['season']
    # 训练标签数据
    train_label = train_data['sum_recv_money']
    train_features = train_data
    # 给特征训练数据添加weekend列
    train_features['weekend'] = [isWeekend(i) for i in train_features["settle_biz_week"].tolist()]
    # 给特征训练数据添加节日列
    train_features['festival'] = [isFestival(i) for i in train_features["settle_biz_date"].tolist()]
    # 添加节前特征列
    train_features['before_festival'] = [beforeFestival(i) for i in train_features["settle_biz_date"].tolist()]
    # 添加节后特征列
    train_features['after_festival'] = [afterFestival(i) for i in train_features["settle_biz_date"].tolist()]
    # 删除无用列
    del train_features['settle_biz_date']
    del train_features['sum_recv_money']
    log.logger.info('Train features data ready')

    lines_num_predict = weather_predict.iloc[:, 0].size  # 统计数据量
    log.logger.info('Number of predict features data is: ' + str(lines_num_predict))
    # 给预测数据添加星期列
    weather_predict['settle_biz_week'] = [date_to_weekday_transfer(i) for i in
                                          weather_predict['settle_biz_date'].tolist()]
    # 给预测数据添加月份列
    weather_predict['month'] = [month_transfer(i) for i in weather_predict['settle_biz_date'].tolist()]
    # 给预测数据添加季节列
    weather_predict['season'] = [season_transfer(i) for i in weather_predict['month'].tolist()]
    # 生成特征预测数据
    predict_features = weather_predict
    # 给特征预测数据添加weekend列
    predict_features['weekend'] = [isWeekend(i) for i in predict_features["settle_biz_week"].tolist()]
    # 给特征预测数据添加节日列
    predict_features['festival'] = [isFestival(i) for i in predict_features["settle_biz_date"].tolist()]
    # 添加节前特征列
    predict_features['before_festival'] = [beforeFestival(i) for i in predict_features["settle_biz_date"].tolist()]
    # 添加节后特征列
    predict_features['after_festival'] = [afterFestival(i) for i in predict_features["settle_biz_date"].tolist()]
    log.logger.info('Predict features data ready')
    return train_features, train_label, predict_features, train_features_reserved

# xgboost模型
def xgboost_model(train_features, train_label, predict_features):
    log.logger.info('Start xgboost model')
    # 预测数据的条数
    predict_features_num = predict_features.iloc[:, 0].size
    # 获取预留的天气预测数据，最后汇总结果用
    predict_features_reserved = predict_features.copy()
    # 删除日期列
    del predict_features['settle_biz_date']
    # 将训练数据和预测数据上下拼接在一起
    all_data = pd.concat([train_features, predict_features], axis=0).reset_index(drop=True)
    log.logger.info('Concat train features data and predict features data ready')
    # 对全部数据的特征进行one-hot处理
    all_data_ontHot = pd.get_dummies(all_data, columns=['settle_biz_week', 'month', 'season',
                                                        'dayweather', 'nightweather', 'daywindforce',
                                                        'weekend', 'festival', 'before_festival', 'after_festival'])
    log.logger.info('All data one-hot ready')
    # 预测数据one_hot
    predict_features_oneHot = all_data_ontHot.tail(predict_features_num).reset_index(drop=True)
    # 训练数据one_hot
    train_features_oneHot = all_data_ontHot.drop(list(all_data.tail(predict_features_num).index.values))
    # 将数据划分为训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(train_features_oneHot, train_label, test_size=0.05)
    log.logger.info('Split train data and test data ready')

    # 模型参数设置
    params = {
        'booster': 'gbtree',  # 迭代模型，还有gbliner
        'objective': 'reg:squarederror',  # gamma, logistic, linear, squarederror
        'eval_metric': 'rmse',  # 损失函数，rmse或mae
        'max_depth': 5,  # 树的最大深度（一般取值3~10），降低模型复杂度，防止过拟合。取值范围 1-无穷
        'min_child_weight': 7,  # 最小样本权重的和。防止过拟合，过大则欠拟合。取值范围 0-无穷
        # 'gamma': 0.1,  # 节点分裂所需损失函数下降的最小值，大于此值才分裂，防止过拟合，控制后剪枝。取值范围 0-无穷。（没有效果？可能是max_depth的作用）
        'subsample': 0.8,  # 行采样，每棵树随机采样数据的比例，(一般取值0.5-1)，增加随机性。取值范围 0-1
        'colsample_bytree': 0.8,  # 列采样，每棵树随机采样特征的比例，(一般取值0.5-1)，增加随机性。取值范围 0-1
        'alpha': 0,  # L1正则化项系数，一般为0
        'lambda': 3,  # L2正则化项系数
        'eta': 0.1,  # 学习率，一般取值范围：0.01-0.2
        'scale_pos_weight': 1,  # 默认值为1，各类别样本十分不均衡时该参数设为正值，可使算法更快收敛
        'silent': 1,  # 是否输出训练过程
        'nthread': 4  # 线程数
    }

    # 准备训练数据
    dtrain = xgb.DMatrix(x_train, y_train)  # 训练集
    log.logger.info('Train DMatrix ready, number is : ' + str(dtrain.num_row()))

    log.logger.info('Start cross validation')
    # 交叉验证
    cv_result = xgb.cv(params,
                       dtrain,
                       nfold=10,
                       num_boost_round=150,
                       early_stopping_rounds=10,
                       # verbose_eval=10,
                       # metrics='rmse',
                       show_stdv=False)

    '''
    cv_res.shape[0]为最佳迭代次数
    print('最佳迭代次数：%d' % cv_result.shape[0])
    定义模型。迭代 num_boost_round 次
    '''
    model = xgb.train(params=params, dtrain=dtrain, num_boost_round=cv_result.shape[0])
    log.logger.info('Model training complete')

    importance = model.get_fscore()  # 特征重要性得分
    log.logger.info('Feature importance score complete')

    # 模型输出结果，使用predict_features_oneHot作为预测模型的输入数据
    result = model.predict(xgb.DMatrix(predict_features_oneHot), ntree_limit=model.best_iteration)
    log.logger.info('Model predict complete')

    # 将list中的元素转换成整数
    result = [int(i) for i in result.tolist()]
    # 给预测预留数据增加结果列，返回最终的预测结果
    predict_features_reserved['sum_recv_money'] = result  # 预测结果生成DataFrame
    return predict_features_reserved, importance

# 拼接最终结果
def get_final_result(train_features_reserved, predict_features_reserved, importance):
    log.logger.info('Start splicing final result')
    result_temp = []
    result_temp.append(train_features_reserved)
    result_temp.append(predict_features_reserved)
    feature_score = get_feature_importance(importance)  # 特征重要性得分结果
    # 遍历历史结果和预测结果
    result_list = {}
    weather = []
    for result in result_temp:
        for index, row in result.iterrows():
            date_day = row['settle_biz_date']
            m_weather = row['dayweather']  # 将天气label转换成文字
            n_weather = row['nightweather']
            m_temper = row['daytemperature']
            n_temper = row['nighttemperature']
            m_wind = row['daywindforce']
            sum_recv_money = row['sum_recv_money']
            # 将结果组成JSON
            dict_temp = {'date': str(date_day), 'sum_recv_money': str(sum_recv_money),
                         'm_weather': str(m_weather), 'n_weather': str(n_weather),
                         'm_temper': str(m_temper), 'n_temper': str(n_temper),
                         'm_wind': str(m_wind)}
            weather.append(dict_temp)
            log.logger.info('One result is: ' + str(dict_temp))

    result_list["weather"] = weather
    result_number = len(weather)
    log.logger.info('Number of final result is: ' + str(result_number))
    result_list["feature_score"] = feature_score
    log.logger.info('Feature score is: ' + str(feature_score))
    if result_number > 0:
        return json.dumps(result_list, ensure_ascii=False)
    else:
        log.logger.info('NO matched result')
        return ''


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
    day_today = '2018-10-11'  # 今天的日期，2019-08-13， 正式环境需删除
    now_str = '2018-10-11 16:45:14'  # 正式环境需删除
    now = datetime.datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S')  # 正式环境需删除
    # day_today = str(datetime.date.today())  # 今天的日期，2019-08-13
    # now = datetime.datetime.now()  # 当前日期时间，2019-08-13 16:45:14.787010
    delta = datetime.timedelta(days=(predict_days - 1))  # 6或14
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
    if len(windforce_str) > 0:
        if windforce_str == '-':
            windforce = 0
        elif windforce_str == '无持续风向微风':
            windforce = 0
        else:
            windforce = re.findall(r"\d", windforce_str)[0]
        return windforce
    else:
        return 0

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
    else:
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

# 判断一天是否是节日
def isFestival(date_str):
    year = date_str.split('-')[0]  # 获取年份
    # 节日
    festivals_2016 = ['2016-01-01', '2016-02-14', '2016-02-07', '2016-02-08',
                      '2016-02-09', '2016-02-10', '2016-02-11', '2016-02-12', '2016-02-13',
                      '2016-02-22', '2016-03-08', '2016-04-02', '2016-04-03', '2016-04-04',
                      '2016-04-30', '2016-05-01', '2016-05-02', '2016-06-01', '2016-06-09',
                      '2016-06-10', '2016-06-11', '2016-06-12', '2016-08-09', '2016-09-15',
                      '2016-09-16', '2016-09-17', '2016-10-01', '2016-10-02', '2016-10-03',
                      '2016-10-04', '2016-10-05', '2016-10-06', '2016-10-07', '2016-11-24',
                      '2016-12-24', '2016-12-25', '2016-12-31']

    festivals_2017 = ['2017-01-01', '2017-01-27',
                      '2017-02-14', '2017-02-28', '2017-01-29', '2017-01-30', '2017-02-01',
                      '2017-02-02', '2017-02-11', '2017-03-08', '2017-04-02', '2017-04-03',
                      '2017-04-04', '2017-04-29', '2017-04-30', '2017-05-01', '2017-06-01',
                      '2017-05-28', '2017-05-29', '2017-05-30', '2017-08-28', '2017-10-04',
                      '2017-10-01', '2017-10-02', '2017-10-03', '2017-10-05', '2017-10-06',
                      '2017-10-07', '2017-10-08', '2017-11-23', '2017-12-24', '2017-12-25',
                      '2017-12-31']

    festivals_2018 = ['2018-01-01', '2018-02-14', '2018-02-15',
                      '2018-02-16', '2018-02-17', '2018-02-18', '2018-02-19', '2018-02-20',
                      '2018-02-21', '2018-03-02', '2018-03-08', '2018-04-05', '2018-04-06',
                      '2018-04-07', '2018-04-29', '2018-04-30', '2018-05-01', '2018-06-01',
                      '2018-06-16', '2018-06-17', '2018-06-18', '2018-08-17', '2018-09-22',
                      '2018-09-23', '2018-09-24', '2018-10-01', '2018-10-02', '2018-10-03',
                      '2018-10-04', '2018-10-05', '2018-10-06', '2018-10-07', '2018-11-22',
                      '2018-12-24', '2018-12-25', '2018-12-31']

    festivals_2019 = ['2019-01-01', '2019-02-14', '2019-02-04', '2019-02-05', '2019-02-06',
                      '2019-02-07', '2019-02-08', '2019-02-09', '2019-02-10', '2019-02-19',
                      '2019-03-08', '2019-04-05', '2019-04-06', '2019-04-07', '2019-05-01',
                      '2019-05-02', '2019-05-03', '2019-05-04', '2019-06-01', '2019-06-07',
                      '2019-06-08', '2019-06-09', '2019-08-07', '2019-09-13', '2019-09-14',
                      '2019-09-15', '2019-10-01', '2019-10-02', '2019-10-03', '2019-10-04',
                      '2019-10-05', '2019-10-06', '2019-10-07', '2019-11-28', '2019-12-24',
                      '2019-12-25', '2019-12-31']

    # 建立{年份:节日}字典
    festival_days = {'2016': set(festivals_2016), '2017': set(festivals_2017), '2018': set(festivals_2018),
                     '2019': set(festivals_2019)}

    # 判断一天是否是节日
    if date_str in festival_days[year]:
        return 1
    else:
        return 0

# 判断一天是否是节前
def beforeFestival(date_str):
    year = date_str.split('-')[0]  # 获取年份
    # 节日
    before_festival_2016 = ['2016-02-13', '2016-02-06', '2016-02-21', '2016-03-07', '2016-04-01',
                            '2016-04-29', '2016-05-27', '2016-05-31', '2016-06-08', '2016-08-08',
                            '2016-09-14', '2016-09-30', '2016-11-23', '2016-12-23', '2016-12-30']

    before_festival_2017 = ['2017-02-13', '2017-01-26', '2017-02-10', '2017-03-07', '2017-04-01',
                            '2017-04-28', '2017-05-31', '2017-08-27', '2017-09-30', '2017-11-22',
                            '2017-12-23', '2017-12-30', '2017-12-23', '2017-12-30']

    before_festival_2018 = ['2018-02-13', '2018-03-01', '2018-03-07', '2018-04-04', '2018-04-28',
                            '2018-05-31', '2018-06-15', '2018-08-16', '2018-09-21', '2018-09-30',
                            '2018-11-21', '2018-12-23', '2018-12-30']

    before_festival_2019 = ['2019-02-13', '2019-02-03', '2019-02-18', '2019-03-07', '2019-04-04',
                            '2019-04-30', '2019-05-31', '2019-06-06', '2019-08-06', '2019-09-12',
                            '2019-09-30', '2019-11-27', '2019-12-23', '2019-12-30']

    # 建立{年份:节日}字典
    before_festival_days = {'2016': set(before_festival_2016), '2017': set(before_festival_2017),
                            '2018': set(before_festival_2018), '2019': set(before_festival_2019)}

    # 判断一天是否是节日
    if date_str in before_festival_days[year]:
        return 1
    else:
        return 0

# 判断一天是否是节后
def afterFestival(date_str):
    year = date_str.split('-')[0]  # 获取年份
    # 节日
    after_festival_2016 = ['2016-01-02', '2016-02-15', '2016-02-23', '2016-03-09', '2016-04-05',
                           '2016-05-03', '2016-06-02', '2016-06-13', '2016-08-10', '2016-09-18',
                           '2016-10-08', '2016-11-25', '2016-12-26']

    after_festival_2017 = ['2017-01-02', '2017-02-15', '2017-02-03', '2017-02-12', '2017-03-09',
                           '2017-04-05', '2017-05-02', '2017-06-02', '2017-05-31', '2017-08-29',
                           '2017-10-09', '2017-11-24', '2017-12-26']

    after_festival_2018 = ['2018-01-02', '2018-02-15', '2018-02-22', '2018-03-03', '2018-03-09',
                           '2018-04-08', '2018-05-02', '2018-06-02', '2018-06-19', '2018-08-18',
                           '2018-09-25', '2018-10-08', '2018-11-23', '2018-12-26']

    after_festival_2019 = ['2019-01-02', '2019-02-15', '2019-02-11', '2019-02-20', '2019-03-09',
                           '2019-04-08', '2019-05-05', '2019-06-02', '2019-06-10', '2019-08-08',
                           '2019-09-16', '2019-10-08', '2019-11-29', '2019-12-26']

    # 建立{年份:节日}字典
    after_festival_days = {'2016': set(after_festival_2016), '2017': set(after_festival_2017),
                           '2018': set(after_festival_2018), '2019': set(after_festival_2019)}

    # 判断一天是否是节日
    if date_str in after_festival_days[year]:
        return 1
    else:
        return 0

# 日期格式转换, 2018-02-23转换为2018年2月23日
def date_transfer_reverse(date_str):
    year, month, day = date_str.split('-')
    # 处理‘月’
    if month[0] == '0':
        month_new = month[1]
    else:
        month_new = month
    # 处理‘日’
    if day[0] == '0':
        day_new = day[1]
    else:
        day_new = day
    date_str_new = year + '年' + month_new + '月' + day_new + '日'
    return date_str_new

# 温度转换
def temperature_transfer(temperature_str):
    if len(temperature_str) > 0:
        if '℃' in temperature_str:
            temperature = temperature_str.strip('℃')
            return int(temperature)
        elif '-' in temperature_str and len(temperature_str) == 1:
            return 20
        elif "\\" in temperature_str:
            return 20
        else:
            return int(temperature_str)
    else:
        return 20

# 创建日志文件夹
def mkdir(path):
    folder = os.path.exists(path)
    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
    else:
        pass

# 计算特征重要性得分
def get_feature_importance(importance):
    settle_biz_week_temp = []
    month_temp = []
    season_temp = []
    dayweather_temp = []
    nightweather_temp = []
    weekend_temp = []
    festival_temp = []
    daywindforce_temp = []
    after_festival_temp = []
    before_festival_temp = []

    # 按类别存储得分
    for key in importance:
        if 'settle_biz_week' in key:
            settle_biz_week_temp.append(importance[key])
        elif 'month' in key:
            month_temp.append(importance[key])
        elif 'season' in key:
            season_temp.append(importance[key])
        elif 'dayweather' in key:
            dayweather_temp.append(importance[key])
        elif 'nightweather' in key:
            nightweather_temp.append(importance[key])
        elif 'daywindforce' in key:
            daywindforce_temp.append(importance[key])
        elif 'weekend' in key:
            weekend_temp.append(importance[key])
        elif 'festival' in key:
            festival_temp.append(importance[key])
        elif 'before_festival' in key:
            before_festival_temp.append(importance[key])
        elif 'after_festival' in key:
            after_festival_temp.append(importance[key])

    # 建立特征名称对应字典（未使用）
    # feature_name_dict = {'daytemperature': '最高温度', 'nighttemperature': '最低温度', 'settle_biz_week': '星期', 'month': '月份',
    #                      'season': '季节', 'dayweather': '白天天气', 'nightweather': '夜间天气', 'daywindforce': '风力',
    #                      'weekend': '周末', 'festival': '节日', 'before_festival': '节日前', 'after_festival': '节日后'}

    # 建立类别最高得分list
    feature_score = []
    feature_score.append({"name": "daytemperature", "desc": "最高温度", "value": str(importance['daytemperature'])})
    feature_score.append({"name": "nighttemperature", "desc": "最低温度", "value": str(importance['nighttemperature'])})
    if len(settle_biz_week_temp) > 0:
        feature_score.append({"name": "settlebizweek", "desc": "星期", "value": str(max(settle_biz_week_temp))})
    if len(month_temp) > 0:
        feature_score.append({"name": "month", "desc": "月份", "value": str(max(month_temp))})
    if len(season_temp) > 0:
        feature_score.append({"name": "season", "desc": "季节", "value": str(max(season_temp))})
    if len(dayweather_temp) > 0:
        feature_score.append({"name": "dayweather", "desc": "白天天气", "value": str(max(dayweather_temp))})
    if len(nightweather_temp) > 0:
        feature_score.append({"name": "nightweather", "desc": "夜间天气", "value": str(max(nightweather_temp))})
    if len(daywindforce_temp) > 0:
        feature_score.append({"name": "daywindforce", "desc": "风力", "value": str(max(daywindforce_temp))})
    if len(weekend_temp) > 0:
        feature_score.append({"name": "weekend", "desc": "周末", "value": str(max(weekend_temp))})
    if len(festival_temp) > 0:
        feature_score.append({"name": "festival", "desc": "节日", "value": str(max(festival_temp))})
    if len(before_festival_temp) > 0:
        feature_score.append({"name": "beforefestival", "desc": "节前", "value": str(max(before_festival_temp))})
    if len(after_festival_temp) > 0:
        feature_score.append({"name": "afterfestival", "desc": "节后", "value": str(max(after_festival_temp))})

    # 按得分对结果进行排序
    feature_score_sorted = sorted(feature_score, key=lambda keys: int(keys['value']), reverse=True)
    return feature_score_sorted


if __name__ == '__main__':
    # 创建日志存放路径
    global file_path
    file_path = "/data/logs/predict/"
    mkdir(file_path)

    # 日志实例
    log = Logger(file_path + 'xgboost.log', level='info')

    try:
        # params = {'city':'天津市',
        #           'group_code':'3297,656,9759',
        #           'store_name':'九田家天津蓟州区蓟县店,九田家天津东丽区复地活力广场店',
        #           'history_date':'2017-09-17,2019-09-17',
        #           'predict_days':7,'all_data':0}



        log.logger.info('xgboost sales predict script start running')
        # 获取传递的参数
        temp = []
        for i in range(len(sys.argv)):
            if i == 1:
                temp.append(sys.argv[i])
        # 参数字符编码格式转换
        temp_str = temp[0].encode('utf-8', errors='surrogateescape').decode('utf-8')
        params = demjson.decode(temp_str)
        log.logger.info('Sending params: ' + str(params))

        # conn = connect(host='192.168.44.82', port=21050)  # 正式环境
        conn = connect(host='192.168.12.204', port=21050)  # 测试环境
        log.logger.info('Connect impala: ' + str(conn))

        weather_predict = Predict(params, conn).query_weather_predict()  # 天气预测数据
        date_train = Predict(params, conn).query_date()  # 营业额训练数据
        weather_train = Predict(params, conn).query_weather_train()  # 天气训练数据

        # 特征训练数据、特征标签、特征预测数据、训练数据预留
        train_features, train_label, predict_features, train_features_reserved = combine_train_data(weather_train, date_train, weather_predict)
        predict_result, importance = xgboost_model(train_features, train_label, predict_features)  # 预测结果
        result = get_final_result(train_features_reserved, predict_result, importance)  # 返回最终结果
        print(result)
        conn.close()  # 关闭连接
        log.logger.info('ALL process complete')

    except Exception as e:
        conn.close()
        log.logger.exception(e)
        Logger(file_path + 'xgboost_error.log', level='error').logger.exception(e)

    finally:
        conn.close()

    # 测试请求参数
    # "{'city':'天津市','group_code':'3297,656,9759','store_name':'九田家天津蓟州区蓟县店,九田家天津东丽区复地活力广场店,九田家天津南开区奥城店,九田家天津红桥区欧亚达店,九田家天津滨海新区八角楼店,九田家天津南开区万德庄店,九田家天津滨海新区塘沽洞庭路一号店','history_date':'2017-09-17,2019-09-17','predict_days':7,'all_data':0}"
