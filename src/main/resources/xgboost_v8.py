#coding=utf-8
from loguru import logger
import os
import sys
import json
import demjson
import datetime
import re
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ============================== 数据预处理类 ==============================
class preproscess:
    @logger.catch
    def __init__(self, params):
        self.city = get_items_sqlStr(params['city'])  # 城市,可多选
        self.groupCode = get_groupCode_sqlStr(params['group_code'])  # 集团号，可多选
        self.storeName = get_items_sqlStr(params['store_name'])  # 门店名，可多选
        self.startDate, self.endDate = get_history_date(params['history_date'])  # 历史数据起止时间
        self.predictDays = params['predict_days']  # 预测天数
        self.allData = params['all_data']  #选择历史2年数据 
        # impala连接配置
        self.impala = {'host': params['impala_host'], 'port': params['impala_port'],
                       'weatherTable': params['weatherTable'], 'salesTable': params['salesTable'],
                       'holidayTable': params['holidayTable']}

    # 获取天气数据
    @logger.catch
    def get_weather(self):
        # 获取未来n天日期
        start_date_pre, end_date_pre, predict_dates_list = get_future_dates(self.predictDays)
        # 判断天气数据查询起止日期，获取sql
        if self.allData == 0:
            weather_sql = get_weather_sql(self.impala['weatherTable'], self.city, self.startDate, end_date_pre)
        if self.allData == 1:
            ndays_early_date = getBeforeDate(730)  # 1年前的日期
            weather_sql = get_weather_sql(self.impala['weatherTable'], self.city, ndays_early_date, end_date_pre)
        logger.info('[SQL]:{weather_sql}', weather_sql=weather_sql)
        # 连接impala
        conn = connect(host=self.impala['host'], port=self.impala['port'])
        with conn.cursor() as cur:
            cur.execute(weather_sql)  # 执行查询
            weather_data = as_pandas(cur)  # 查询结果
        weather_data = weather_data.dropna(axis=0, how='any').reset_index(drop=True)  # 去空值
        # 天气数据判断
        if weather_data.iloc[:, 0].size > 0:
            # 获取查询结果数据的起止日期
            first_weather_date = weather_data['record_date'].head(1).tolist()[0]
            last_weather_date = weather_data['record_date'].tail(1).tolist()[0]
            # 判断没有未来天气数据
            if last_weather_date < start_date_pre:
                print('')
                logger.info('[Tips]:no matched future weather data!')
                sys.exit()
            # 判断没有历史数据
            if first_weather_date >= start_date_pre:
                print('')
                logger.info('[Tips]:no matched history weather data!')
                sys.exit()
            # 转换风力格式
            weather_data['wind_force'] = [windforce_transfer(i) for i in weather_data['wind_force'].tolist()]
            logger.info('[Tips]:weather data ready.')
            return weather_data, start_date_pre
        else:
            print('')
            logger.info('[Tips]:no matched weather data!')
            sys.exit()

    # 获取营业额数据
    @logger.catch
    def get_sales(self):
        # 判断天气数据查询起止日期，获取sql
        if self.allData == 0:
            sales_sql = get_sales_sql(self.impala['salesTable'], self.groupCode, self.storeName, self.startDate, self.endDate)
        if self.allData == 1:
            ndays_early_date = getBeforeDate(730)  # 2年前的日期
            sales_sql = get_sales_sql(self.impala['salesTable'], self.groupCode, self.storeName, ndays_early_date, self.endDate)
        logger.info('[SQL]:{sales_sql}', sales_sql=sales_sql)
        # 连接impala
        conn = connect(host=self.impala['host'], port=self.impala['port'])
        with conn.cursor() as cur:
            cur.execute(sales_sql)  # 执行查询
            sales_data = as_pandas(cur)  # 查询结果
        sales_data = sales_data.dropna(axis=0, how='any').reset_index(drop=True)  # 去空值
        if sales_data.iloc[:, 0].size > 0:
            logger.info('[Tips]:sales data ready.')
            return sales_data
        else:
            logger.info('[Tips]:No matched sales data!')
            print('')
            sys.exit(0)

    # 获取节假日数据
    @logger.catch
    def get_holiday(self):
        holiday_sql = get_holiday_sql(self.impala['holidayTable'])
        # 连接impala
        conn = connect(host=self.impala['host'], port=self.impala['port'])
        with conn.cursor() as cur:
            cur.execute(holiday_sql)  # 执行查询
            holiday_data = as_pandas(cur)  # 查询结果
        if holiday_data.iloc[:, 0].size > 0:
            # 转换日期格式为字符串
            holiday_data['holiday_date'] = [i.strftime('%Y-%m-%d') for i in holiday_data['holiday_date']]
            # 节前数据
            before_festival = holiday_data[holiday_data['holiday_name'] == '节前']
            del before_festival['holiday_name']
            before_festival.columns = ['record_date', 'before_festiva']
            # 节后数据
            after_festival = holiday_data[holiday_data['holiday_name'] == '节后']
            del after_festival['holiday_name']
            after_festival.columns = ['record_date', 'after_festival']
            # 节日数据
            festival = holiday_data.drop(before_festival.index).drop(after_festival.index)  # 节日数据
            del festival['holiday_name']
            festival.columns = ['record_date', 'festival']
            # 获取分类后的节日dataframe
            temp = pd.merge(festival, before_festival, on='record_date', how='outer')
            holiday_data = pd.merge(temp, after_festival, on='record_date', how='outer').fillna(0)
            logger.info('[Tips]:holiday data ready.')
            return holiday_data
        else:
            logger.info('[Tips]:holiday data is empty!')
            print('')
            sys.exit(0)

# 获取历史预留15天数据
@logger.catch
def get_history_15(weather_data, sales_data, start_date_pre):
    weather_hist = weather_data[weather_data['record_date'] < start_date_pre]  # 历史天气数据
    weather_pre = weather_data[weather_data['record_date'] >= start_date_pre]  # 未来天气数据
    history_15 = []  # 存储15天预留数据
    # 拼接天气数据和营业额数据
    combine_data = pd.merge(weather_hist, sales_data, how='left', on='record_date') \
                     .dropna(axis=0, how='any').reset_index(drop=True)
    combine_count = combine_data.iloc[:, 0].size  # 统计数据量
    if combine_count == 0:
        logger.info('[Tips]:No matched reserved data!')
        print('')
        sys.exit()
    if 0 < combine_count < 15:
        logger.info('[Count]:[reserved-data count]: {reserved_count}', reserved_count=combine_count)
        reserved = combine_data
    else:
        reserved = combine_data.tail(15)
    for index, row in reserved.iterrows():
        temp_dict = {'date': str(row['record_date']), 'sum_recv_money': str(int(row['sales'])),
                     'm_weather': str(row['day_weather']), 'n_weather': str(row['night_weather']),
                     'm_temper': str(row['day_temp']), 'n_temper': str(row['night_temp']),
                     'm_wind': str(row['wind_force'])}
        history_15.append(temp_dict)
    logger.info('[Tips]:History reserved data ready.')
    return history_15, weather_pre

# 组合训练数据
@logger.catch
def get_feature_data(holiday_data, weather_data_original, sales_data, start_date_pre):
    # 添加月份列
    weather_data_original['month'] = [month_transfer(i) for i in weather_data_original['record_date'].tolist()]
    # 添加季节列
    weather_data_original['season'] = [season_transfer(i) for i in weather_data_original['month'].tolist()]
    # 添加周末列
    weather_data_original['weekend'] = [weekend_transfer(i) for i in weather_data_original['week_day'].tolist()]
    # 添加节日列
    weather_data = pd.merge(weather_data_original, holiday_data, on='record_date', how='left').fillna(0)
    # one-hot编码天气数据
    weather_oht = pd.get_dummies(weather_data, columns=['week_day', 'month', 'season', 'weekend',
                                                        'day_weather', 'night_weather',
                                                        'festival', 'before_festiva', 'after_festival'])
    weather_hist = weather_oht[weather_oht['record_date'] < start_date_pre]  # 历史天气数据
    weather_pre = weather_oht[weather_oht['record_date'] >= start_date_pre]  # 未来天气数据
    # 拼接日期数据和天气数据
    train_features = pd.merge(weather_hist, sales_data, how='left', on='record_date')\
                   .dropna(axis=0, how='any').reset_index(drop=True)  # 训练数据
    train_lable = train_features['sales']  # 训练标签数据
    pre_features = weather_pre  # 预测数据
    # 删除训练数据无用列
    del train_features['record_date']
    del train_features['sales']
    del pre_features['record_date']
    return train_features, train_lable, pre_features

# xgboost模型
@logger.catch
def xgboost_model(train_features, train_lable, pre_features, weather_pre):
    predict_result = weather_pre.copy()  # 预测结果的天气部分
    # 将数据划分为训练集和测试集
    x_train, x_test, y_train, y_test = train_test_split(train_features, train_lable, test_size=0.05)
    # 模型参数设置
    params = {
        'booster': 'gbtree',  # 迭代模型，还有gbliner
        'objective': 'reg:squarederror',  # gamma, logistic, linear, squarederror
        'eval_metric': 'rmse',  # 损失函数，rmse或mae
        'max_depth': 5,  # 树的最大深度（一般取值3~10），降低模型复杂度，防止过拟合。取值范围 1-无穷
        'min_child_weight': 7,  # 最小样本权重的和。防止过拟合，过大则欠拟合。取值范围 0-无穷
        # 'gamma': 0.1,  # 节点分裂所需损失函数下降的最小值，大于此值才分裂，防止过拟合，控制后剪枝。取值范围 0-无穷
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
    # 交叉验证
    cv_result = xgb.cv(params,
                       dtrain,
                       nfold=10,
                       num_boost_round=150,
                       early_stopping_rounds=10,
                       show_stdv=False)
    logger.info('[Tips]:Cross validation complete.')
    # num_boost_roun为最多迭代次数，cv_res.shape[0]为最佳迭代次数。
    model = xgb.train(params=params, dtrain=dtrain, num_boost_round=cv_result.shape[0])   # 训练模型
    logger.info('[Tips]:Model training complete.')
    fscore = model.get_fscore()  # 计算特征得分
    logger.info('[Tips]:Feature importance score complete.')
    result = model.predict(xgb.DMatrix(pre_features), ntree_limit=model.best_iteration)  # 预测输出结果
    logger.info('[Tips]:Prediction complete.')
    # 给预测预留数据增加结果列，返回最终的预测结果
    result = [int(i) for i in result.tolist()]
    predict_result['sales'] = result
    prediction = []  # 存储预测结果
    for index, row in predict_result.iterrows():
        temp_dict = {'date': str(row['record_date']), 'sum_recv_money': str(row['sales']),
                     'm_weather': str(row['day_weather']), 'n_weather': str(row['night_weather']),
                     'm_temper': str(row['day_temp']), 'n_temper': str(row['night_temp']),
                     'm_wind': str(row['wind_force'])}
        prediction.append(temp_dict)
    logger.info('[Tips]:XGBoost model predict complete.')
    return prediction, fscore

# 计算特征重要性得分
@logger.catch
def get_feature_score(fscore):
    # 特征得分为空的情况
    if len(fscore) == 0:
        logger.info('[Tips]:Feature score result is empty.')
        feature_score_sorted = []
    else:
        # 特征描述
        feature_desc = {'week_day': '星期', 'month': '月份', 'season': '季节', 'weekend': '周末', 'wind_force': '风力',
                        'day_temp': '最高温度', 'night_temp': '最低温度', 'day_weather': '白天天气', 'night_weather': '夜间天气',
                        'festival': '节日', 'beforefestival': '节前', 'afterfestival': '节后'}
        # 特征得分dict转换成list
        feature_score_list = []
        for key, value in fscore.items():
            if key in ['day_temp', 'night_temp', 'wind_force']:
                feature = key
            else:
                feature = key[0:key.rfind('_', 1)]
            temp = [feature, value]
            feature_score_list.append(temp)
        # 聚合特征的得分情况
        existed_feature = []
        feature_score_dict = {}
        for item in feature_score_list:
            name = item[0]
            value = item[1]
            if name in existed_feature:
                feature_score_dict[name].append(value)
            else:
                feature_score_dict[name] = [value]
                existed_feature.append(name)
        # 给没有用到的特征赋值得分0
        for key, value in feature_desc.items():
            if key not in existed_feature:
                feature_score_dict[key] = [0]
        # 组合最终特征得分结果
        feature_score = []
        for feature, score_list in feature_score_dict.items():
            # 新老特征名称对照dict
            feature_name_dict = {'week_day': 'settlebizweek', 'month': 'month', 'season': 'season', 'weekend': 'weekend',
                                 'wind_force': 'daywindforce', 'day_temp': 'daytemperature', 'night_temp': 'nighttemperature',
                                 'day_weather': 'dayweather', 'night_weather': 'nightweather', 'festival': 'festival',
                                 'beforefestival': 'beforefestival', 'afterfestival': 'afterfestival'}
            score_dict = {}
            max_score = str(max(score_list))
            score_dict['name'] = feature_name_dict[feature]
            score_dict['desc'] = feature_desc[feature]
            score_dict['value'] = max_score
            feature_score.append(score_dict)
        # 特征得分结果按分值排序
        feature_score_sorted = sorted(feature_score, key=lambda keys: int(keys['value']), reverse=True)
        logger.info('[Tips]:feature score ready.')
    return feature_score_sorted

# 组合最终结果
@logger.catch
def get_final_result(history_15, prediction, feature_score):
    result_dict = {}
    weather = history_15 + prediction
    result_count = len(weather)
    if result_count > 0:
        logger.info('[Count]:final-result count:{final_count}', final_count=result_count)
        result_dict['weather'] = weather
        result_dict['feature_score'] = feature_score
        logger.info('[Tips]:all process complete')
        return json.dumps(result_dict, ensure_ascii=False)
    else:
        logger.info('[Tips]:final result is empty!')
        return ''

# =================================== 请求参数处理函数 ===================================
# 获取传递的参数
def get_params(sys_argv):
    temp = []
    for i in range(len(sys_argv)):
        if i == 1:
            temp.append(sys_argv[i])
    # 参数字符编码格式转换
    temp_str = temp[0].encode('utf-8', errors='surrogateescape').decode('utf-8')
    params = demjson.decode(temp_str)
    logger.info('[Params]:{params}', params=params)
    return params

# 拼接字符串参数项的sql语句片段
def get_items_sqlStr(items_str):
    items_list = items_str.split(',')
    items_sqlStr = ','.join(list(map(lambda x: "'%s'" % x, items_list)))
    return items_sqlStr

# 拼接集团号参数项的sql语句片段
def get_groupCode_sqlStr(items_str):
    items_list = items_str.split(',')
    items_sqlStr = ','.join(items_list)
    return items_sqlStr

# 获取历史数据的开始结束时间
def get_history_date(date_str):
    date_list = date_str.split(',')
    start_date = date_list[0]
    end_date = date_list[1]
    return start_date, end_date

# =================================== sql语句拼接处理函数 ===================================
# 获取天气sql
def get_weather_sql(weather_db, city, start_date, end_date_pre):
    weather_sql = "SELECT " \
                      "w.record_date," \
                      "w.week_day," \
                      "w.day_weather," \
                      "w.night_weather," \
                      "w.day_temp," \
                      "w.night_temp," \
                      "w.wind_force " \
                  "FROM " \
                      "(SELECT " \
                          "from_timestamp(recorddatetarget,'yyyy-MM-dd') AS record_date," \
                          "DAYOFWEEK(recorddatetarget) AS week_day," \
                          "CAST(dayweatherfeature AS INT) AS day_weather," \
                          "CAST(nightweatherfeature AS INT) AS night_weather," \
                          "daytemperature_format AS day_temp," \
                          "nighttemperature_format AS night_temp," \
                          "daywindforce as wind_force," \
                          "ROW_NUMBER() OVER (PARTITION BY recorddatetarget ORDER BY recorddatetarget DESC) AS tb_rank " \
                      "FROM " \
                          "{weather_db} ".format(weather_db=weather_db) + \
                      "WHERE " \
                          "city LIKE CONCAT(REPLACE({city},'市','%'),'%')".format(city=city) + \
                          "AND recorddatetarget BETWEEN '{start_date}' AND '{end_date_pre}' ".format(start_date=start_date, end_date_pre=end_date_pre) + \
                      "ORDER BY " \
                          "recorddatetarget) w " \
                  "WHERE " \
                    "w.tb_rank = 1 " \
                  "ORDER BY " \
                    "w.record_date"
    return weather_sql

# 获取营业额sql
def get_sales_sql(sales_db, group_code, store_name, start_date, end_date):
    sales_sql = "SELECT " \
                        "settle_biz_date AS record_date, " \
                        "sum(recv_money) AS sales " \
                "FROM " \
                        "{sales_db} ".format(sales_db=sales_db) + \
                "WHERE " \
                        "group_code IN ({group_code}) ".format(group_code=group_code) + \
                        "AND store_name IN ({store_name}) ".format(store_name=store_name) +  \
                        "AND settle_biz_date BETWEEN '{start_date}' AND '{end_date}' ".format(start_date=start_date, end_date=end_date) + \
                "GROUP BY " \
                        "record_date " \
                "ORDER BY " \
                        "record_date ASC"
    return sales_sql

# 获取节假日sql
def get_holiday_sql(holiday_db):
    holiday_sql = "SELECT " \
                        "holiday_name," \
                        "holiday_date," \
                        "effect_level " \
                  "FROM " \
                        "{holiday_db} ".format(holiday_db=holiday_db) + \
                  "ORDER BY " \
                        "holiday_date"
    return holiday_sql
# =================================== 转换函数 ===================================
# 获取今天日期、7天后的日期(包含今天)、未来7天的日期列表
def get_future_dates(predict_days):
    day_today = '2018-12-15'  # 今天的日期，2019-08-13， 测试用，正式环境需删除
    now_str = '2018-12-15 16:45:14'  # 测试用，正式环境需删除
    now = datetime.datetime.strptime(now_str, '%Y-%m-%d %H:%M:%S')  # 测试用，正式环境需删除
    # day_today = str(datetime.date.today())  # 今天的日期，2019-08-13  # 正式环境用
    # now = datetime.datetime.now()  # 当前日期时间，2019-08-13 16:45:14.787010  # 正式环境用
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

# 获取N天前的日期
def getBeforeDate(beforeDays):
    today_str = '2018-12-15 10:32:52'  # 测试用，正式环境需删除
    today = datetime.datetime.strptime(today_str, '%Y-%m-%d %H:%M:%S')  # 测试用，正式环境需删除
    # today = datetime.datetime.now()  # 正式环境用
    # 计算偏移量
    offset = datetime.timedelta(days=-beforeDays)
    # 获取想要的日期的时间
    before_date = (today + offset).strftime('%Y-%m-%d')
    return before_date

# 将'<3级'风力转换成数字格式
def windforce_transfer(windforce_str):
    if len(windforce_str) > 0:
        if windforce_str == '-':
            windforce = 0
        elif windforce_str == '无持续风向微风':
            windforce = 0
        else:
            windforce = re.findall(r"\d", windforce_str)[0]
        return int(windforce)
    else:
        return 0

# 根据日期获取月份
def month_transfer(date_str):
    month = date_str.split('-')[1]
    return int(month)

# 根据月份获取季节
def season_transfer(month):
    season_dict = {'12': 4, '1': 4, '2': 4,
                    '3': 1, '4': 1, '5': 1,
                    '6': 2, '7': 2, '8': 2,
                    '9': 3, '10': 3, '11': 3}
    return season_dict[str(month)]

# 根据星期几获取周末
def weekend_transfer(weekday):
    if weekday == 1 or weekday == 7:
        return 1
    else:
        return 0

# ================================== 程序入口 ==================================
if __name__ == '__main__':
    try:
        t1 = datetime.datetime.now()
        # 日志路径
        logs_path = '/data/logs/predict/store_turnover_predict.log'
        logger.add(logs_path, rotation='00:00', enqueue=True)
        logger.info('=============== Store turnover predict start')

        # 获取请求参数
        params = get_params(sys.argv)

        # 获取天气数据
        weather_data, start_date_pre = preproscess(params).get_weather()
        # 获取营业额数据
        sales_data = preproscess(params).get_sales()
        # 获取节假日数据
        holiday_data = preproscess(params).get_holiday()
        # 获取最终结果预留数据
        history_15, weather_pre = get_history_15(weather_data, sales_data, start_date_pre)
        # 组合训练数据
        train_features, train_lable, pre_features = get_feature_data(holiday_data, weather_data, sales_data, start_date_pre)
        # 获取xgboost模型训练，预测结果
        prediction, fscore = xgboost_model(train_features, train_lable, pre_features, weather_pre)
        # 获取特征得分
        feature_score = get_feature_score(fscore)
        # 组合最终输出结果
        result = get_final_result(history_15, prediction, feature_score)
        print(result)
        t2 = datetime.datetime.now()
        logger.info('[Time]:script run time:{run_time}', run_time=t2-t1)

    except Exception as e:
        print('')
        logger.exception(e)
        sys.exit()

    # 测试请求参数
    # "{'city': '长沙市','group_code': '3297,9759','store_name': '九田家湖南长沙天心区汇金国际店,九田家湖南长沙岳麓区梅溪大街店','history_date': '2017-10-01,2018-12-15','predict_days': 7,'all_data': 0,'impala_host': '192.168.12.204','impala_port': 21050,'weatherTable': 'worm.weather','salesTable': 'e000.dw_trade_bill_fact_p_group_upgrade','holidayTable': 'ocmanage.holiday'}"