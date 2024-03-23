import pandas as pd
from datetime import date, datetime, timedelta
CREDS_FOLDER = 'ПУТЬ К ПАПКЕ С КРЕДАМИ'
CREDS_FILE = "ЗДЕСЬ ИМЯ json-ФАЙЛА С КРЕДАМИ К BQ"
ENDPOINT_FORECAST = "http://IP ВИРТУАЛЬНОЙ МАШИНЫ/f"


# Класс взаимодействия с BQ
class workWithBQ(object): 
    name = 'WorkWithBQ'
    def __init__(self, project_id = 'ID ПРОЕКТА В BQ', creds_folder = CREDS_FOLDER, creds_file = CREDS_FILE, forecast_marker = None, sender = None, comment_text = None):
        try:
            from google.oauth2 import service_account
        except:
            return('Проблема загрузки сервисного аккаунта. Необходимо установить библиотеку google.oauth2')
        
        # ИНИЦИАЛИЗИРУЕМ СЕРВИСНЫЕ ДОСУПЫ
        self.credentials_bq_write = service_account.Credentials.from_service_account_file(creds_folder + '/' + creds_file)
        self.project_id = project_id
        self.forecast_marker = forecast_marker
        self.sender = sender
        self.comment_text = comment_text

    # Функция чтения из BQ
    def readBySQL(self, sql, need_to_check = True):
        try:
            self.sended_sql = sql # Сохраняем отправленный sql
            result = pd.read_gbq(sql, project_id = self.project_id, credentials = self.credentials_bq_write)
        except Exception as e :
            print ('Ошибка выполнения запроса' + str(e))
            result = pd.DataFrame({'error':['sql is worng']})
        return result
    
           
# Класс для прогнозирования
class getForecast(object):
    from fbprophet import Prophet    
    
    def __init__(self, 
                 df = None, 
                 sql_query = None, 
                 fake_yesterday = None, 
                 forecastMarker = None, 
                 error_type = 'mape', 
                 sender = None, 
                 forecasting_period = 365, 
                 use_hypo = True, 
                 sub_forecasts_as_regressor = '', 
                 get_lower_and_upper = False, 
                 interval_width = 0.8,
                 error_output = False):
        import hashlib
        
        # Инициализируем свойства класса   
        self.df = df
        self.forecast_unic_id = str(datetime.today()) # Уникальный идентификатор прогноза. Используется для дозаписи характеристик прогнозирования
        self.error_type = error_type
        self.forecastMarker = forecastMarker
        self.fake_yesterday = fake_yesterday
        self.sender = sender
        self.sql_query = sql_query
        self.using_sql_query = False # Указывает что в прогнозе использовался sql запрос, а не датафрейм
        self.sql_from_hypo = False # Указывает что в качестве sql запроса был взят запрос из таблицы гиперпараметров
        self.forecasting_period = forecasting_period
        self.use_hypo = use_hypo
        self.sub_forecasts_as_regressor = sub_forecasts_as_regressor.replace(' ','').split(',') 
        self.session_id = hashlib.md5(self.forecast_unic_id.encode()).hexdigest()
        self.get_lower_and_upper = get_lower_and_upper
        self.interval_width = interval_width
        self.comment_text = None
        self.error_output = error_output
        
        # Получаем гиперпараметры и регрессоры, если указана задача прогнозирования
        if (forecastMarker is None):
            # Если не надо получить прогноз с конкретными параметрами.
            self.parametrs_in_model = None
            self.regressors_in_model = ['dow']
            
            # Сохраняем состояние прогноза со специальными параметрами
            self.isSpecialParametrs = False
            
            # Так как не искали по таблице гиперпараметров, то количество строк возвращаем как 0
            self.found_hypo_records = 0

        else:
            # Если надо получить параметры для конкретного набора данных
            self.parametrs_in_model, self.regressors_in_model, self.found_hypo_records, sql_in_hypo, sub_forecasts, self.comment_text = self.getParametrsAndRegressors(forecastMarker)
            
            try:
                self.parametrs_in_model.update({"interval_width" : interval_width})
            except:
                pass
            
            # Сохраняем состояние прогноза со специальными параметрами     
            if self.found_hypo_records >= 1:
                self.isSpecialParametrs = True # Описываем что в модели используются особые параметры
                if ((sql_query is None) & (sql_in_hypo is not None)): # Если sql запрос не передан, но такой запрос есть в таблице гиперпараметров
                    self.sql_query = sql_query = sql_in_hypo # Используем sql запрос
                    self.sql_from_hypo = True
                                
                if ( 
                    (len(self.sub_forecasts_as_regressor) == 1)
                    & 
                    (self.sub_forecasts_as_regressor[0] == '')
                   ): # Если запрос на подпрогноз не пришёл в запросе, но указан таблице с гиперпараметрами
                    self.sub_forecasts_as_regressor = sub_forecasts
                
            else: 
                self.isSpecialParametrs = False        
        
        # Проверяем, если на входе есть sql_запрос, то базовый датафрейм формируем из этого запроса
        if (type(df) == type(None)) & (type(sql_query) != type(None)):       
            #print('Формируем прогноз по sql_query')           
            self.using_sql_query = True
            bq_obj = workWithBQ(
                forecast_marker = self.forecastMarker, 
                sender = self.sender, 
                comment_text = self.comment_text)
            
            df = bq_obj.readBySQL(sql_query) 
            self.sended_sql = bq_obj.sended_sql
            self.df = df#.fillna(0)
        else:
            self.df = df # получаем входящий датафрейм для прогноза
        try:                            
            self.df_size = (df.shape)
        except:
            self.df_size = None
        
        # DOW и регрессоры
        self.dow, self.available_regressors = self.getDOW() # Получаем датафрейм с рабочими и нерабочими днями недели
        
        # Если использованны значения прогноза, как регрессоры, добавляем их в список регрессоров
        if self.sub_forecasts_as_regressor[0] != '':
            self.regressors_in_model += self.sub_forecasts_as_regressor
        
        # Формируем массив для кросс-валидации полученой модели
        self.fake_yesterday, self.df, self.cutoffs = self.getCutoffs(fake_yesterday)
                
        # Получаем прогноз при прогнозировании
        if type(self.df) == type(None): # Если пришёл пустой датафрейм
            self.forecast, self.error_rate, self.m = pd.DataFrame({'ds':['empty_sql','empty_dataframe']}, index = None), -3.14, None
            return None
        
        self.forecast, self.error_rate, self.m = self.get_forecast(self.df, 'mape', param = self.parametrs_in_model, regressors = self.regressors_in_model)
        
        return None
           

    def getCutoffs(self, fake_yesterday):
        self_df = self.df
        if type(self_df)!=type(None):
            self_df['ds'] = self_df['ds'].astype('datetime64[ns]')
        
        if fake_yesterday is None: # Если параметр фейкового вчера не задан
            fake_yesterday = str(date.today())
        else: #если фейковое вчера задано, то выкидываем из датафрейма ненужные даты
            self_df['ds'] = self.df['ds'].astype('datetime64[ns]')
            self_df.drop( # Удаляем даты, которые превышают фейковое вчера
                self_df[self_df['ds'] >= fake_yesterday].index,
                inplace = True
            )
            
        dt = datetime.strptime(fake_yesterday, '%Y-%m-%d') + timedelta(days=-30)
        if type(self_df)!=type(None):
            if (self_df.ds.max() - timedelta(days=30)).date() <= dt.date():
                days = ((self_df.ds.max() - timedelta(days=30)).date() - dt.date()).days
                dt = dt.date()+timedelta(days=days)
        
        self_cutoffs = self.pd.to_datetime([str(dt)[:4] + '-' + str(dt)[5:7] + '-' + str(dt)[8:10]])
        return fake_yesterday, self_df, self_cutoffs

    
    # Получаем гиперпараметры и регрессоры, которые относятся с конкретной задаче
    def getParametrsAndRegressors(self, forecastMarker):
        bq_obj = workWithBQ(
            forecast_marker = 'hyperparameters', 
            sender = self.sender,
            comment_text = 'Обращаемся к таблице гиперпараметров'
        ) # Получаем экземпляр класса работы в BQ
        
        # Обращаемся к таблице, хранящей гиперпараметры
        df_hyper = bq_obj.readBySQL(f""" 
            SELECT * 
            FROM `ID ПРОЕКТА В BQ.forecast.hyperparameters` 
            WHERE forecastMarker = "{forecastMarker}"
            ORDER BY date DESC
            LIMIT 1        
        """)
        
        if df_hyper.shape[0] >= 1: # Если найдены подходящие 
                param = {} #по умолчанию задаём пустой sql-запрос для прогноза
                sql_in_hypo = None
                regressors = ['dow'] # Базовая настройка регрессоров
                sub_forecasts = ['']
                comment_text = None
                
                if self.use_hypo == True: # Если не стоит запрет на использование гиперпараметров
                    if str(df_hyper.loc[0,'changepoint_prior_scale']) != 'nan':
                        param['changepoint_prior_scale'] = df_hyper.loc[0,'changepoint_prior_scale'] 
                    if str(df_hyper.loc[0,'seasonality_prior_scale']) != 'nan':
                        param['seasonality_prior_scale'] = df_hyper.loc[0,'seasonality_prior_scale'] 
                    if str(df_hyper.loc[0,'holidays_prior_scale']) != 'nan':
                        param['holidays_prior_scale'] = df_hyper.loc[0,'holidays_prior_scale'] 
                    if str(df_hyper.loc[0,'seasonality_mode']) != 'None':
                        param['seasonality_mode'] = df_hyper.loc[0,'seasonality_mode'] 
                    if str(df_hyper.loc[0,'regressors']) != 'None': # Если регрессоры найдёны в настройках модели
                        regressors = df_hyper.loc[0,'regressors'].replace(' ','').split(',')
                    else:
                        regressors = ['dow']
                    if str(df_hyper.loc[0,'sub_forecasts']) != 'None':
                        sub_forecasts = df_hyper.loc[0,'sub_forecasts'].replace(' ','').split(',')
                    else:
                        sub_forecasts = ['']

                if str(df_hyper.loc[0,'sql']) != 'None':
                    sql_in_hypo = df_hyper.loc[0,'sql'] 

                if str(df_hyper.loc[0,'comment_text']) != 'None':
                    comment_text = df_hyper.loc[0,'comment_text'] 
                return param, regressors, df_hyper.shape[0], sql_in_hypo, sub_forecasts, comment_text
        else:
            return None, ['dow'], 0, None, None, None
        
    def getDOW(self): # Получаем список доступных регрессоров
        bq_obj = workWithBQ(
            forecast_marker = 'DOW', 
            sender = self.sender,
            comment_text = 'Получаем набор регрессоров'
        ) # Получаем экземпляр класса работы в BQ
        
        # Обращаемся к таблице, в которой хранятся доступные значения регрессоров
        df_regressors = bq_obj.readBySQL(f""" 
            SELECT * 
            FROM `ID ПРОЕКТА В BQ.forecast.regressor` 
            ORDER BY ds ASC
        """)
        
        df_regressors['ds'] = df_regressors['ds'].apply(lambda x: str(x)[:10])
        df_regressors['ds'] = df_regressors['ds'].astype('datetime64[ns]')       

        # Выполняем задачи прогнозирования, если они переданы как регрессоры
        def getForecastMarkerAsRegressor(self, sub_forecasts_as_regressor, available_regressors):
            import json
            import requests
            
            if ( # Если передан пустой массив
                (len(sub_forecasts_as_regressor) == 1) &
                (sub_forecasts_as_regressor[0] == '')
            ):
                return available_regressors
            
            for sub_ft in sub_forecasts_as_regressor:
                print('---' + sub_ft)
                print('sub_ft: ' + str(sub_ft))
                                
                data = {'forecastMarker' : sub_ft, 'sender' : "internal_call for " + str(self.forecastMarker) + ', session_id: '+str(self.session_id)} # data to be sent to api
                r = requests.post(url = ENDPOINT_FORECAST, data = data)
                j = json.loads(r.text) # sending post request and saving response as response object

                result = pd.DataFrame({ # Формируем датафрейм с прогнозом 
                    'ds':j['ds'], sub_ft: j['iliPlanIliFact']
                }) 

                result['ds'] = pd.to_datetime(result['ds'], unit='ms') # Переводим время в микросекундах в формат даты

                available_regressors = available_regressors.join(result.set_index('ds'), on = 'ds') # Подклеиваем полученный результат к набору уже добавленных регрессоров

                # Получаем диапазон заполненных данных для нового регрессора и значения для заполнения (среднее 10 значений)
                last_month = available_regressors[~available_regressors[sub_ft].isna()].iloc[-1]['ds'] # последнее зафиксированное значение
                first_month = available_regressors[~available_regressors[sub_ft].isna()].iloc[0]['ds'] # первое зафиксированное значение

                last_value = available_regressors[~available_regressors[sub_ft].isna()].iloc[-10:-1][sub_ft].mean() # последнее зафиксированное значение
                first_value = available_regressors[~available_regressors[sub_ft].isna()].iloc[0:10][sub_ft].mean() # первое зафиксированное значение

                # Заполняем пропущенные данные значениями, которые в начале и в концесуществующего диапазона 
                available_regressors.loc[available_regressors[available_regressors['ds'] > last_month].index, sub_ft] = last_value # заменям на первое значимое значение всё что было до него
                available_regressors.loc[available_regressors[available_regressors['ds'] < first_month].index, sub_ft] = first_value # заменям на последнее значимое значение всё что было после
            return available_regressors      
        
        df_regressors = getForecastMarkerAsRegressor(self,self.sub_forecasts_as_regressor, df_regressors)
        return df_regressors[['ds', 'dow']], df_regressors

    
    # Основная функция прогнозирования
    def get_forecast(self, result, error_type, **kwargs): 
        result['y'] = result['y'].astype('float')
        result['ds'] = result['ds'].astype('datetime64[ns]')
        
        # Добавляем регрессор по рабочим дням
        result = result.join(self.available_regressors.set_index('ds'), on = 'ds', rsuffix = '') # джоиним столбец с регрессором

        if kwargs.get('param') is None:
            m = self.Prophet() # Указываем модели что надо использовать ежегодно государственые праздники
        else:
            m = self.Prophet(**kwargs['param']) # Указываем модели что надо использовать ежегодно государственые праздники
        
        if kwargs.get('regressors') is None:
            m.add_regressor('dow') # указываем наличие столбца регрессора   
        else: 
            for reg in kwargs.get('regressors'):   
                # Если такой регрессор есть в списке доступных
                if reg in list(self.available_regressors.columns):
                    m.add_regressor(reg)
                
        result['ds'] = result['ds'].astype('datetime64[ns]') # Гигиенически меняем тип данных 

        m.fit(result, seed = 1000) # Построние функции модели
        future = m.make_future_dataframe(periods = self.forecasting_period) # построение временного периода в будущее, в этом примере диапазон 365 дней
        self.dow['ds'] = self.dow['ds'].astype('datetime64[ns]') # Гигиенически меняем тип данных 
        future = future.join(self.available_regressors.set_index('ds'),on='ds',rsuffix='') # джоиним столбец с регрессором
        forecast = m.predict(future) # Получаем прогноз по функции для временного периода
        forecast = forecast.join(result[['ds','y']].set_index('ds'),on='ds',rsuffix = 'fact')
        forecast['y'].fillna('-1',inplace=True)
        forecast['y'] = forecast['y'].astype('float')

        # Для удобства работы формируем ещё один столбец, который содержит микс фактических и прогнозных значений
        forecast['iliPlanIliFact']=0
        i = forecast[forecast['y']!=-1].index
        forecast.loc[i,'iliPlanIliFact']=forecast.loc[i,'y']
        i = forecast[forecast['y']==-1].index
        forecast.loc[i,'iliPlanIliFact']=forecast.loc[i,'yhat']
        
        error_rate = self.get_error_for_model(m, error_type) # Получаем значние ошибки прогноза
        output_mas = ['ds','y','yhat','iliPlanIliFact','yhat_upper', 'yhat_lower'] # базовый набор возвращаемых значений
        
        if self.get_lower_and_upper:
            output_mas += ['yhat_upper', 'yhat_lower']
        
        return  forecast[output_mas], error_rate, m #возвращаем 2 столбца из датафрейма с прогнозом

    # Функция расчета ошибки прогнозирования
    def get_error_for_model(self, m, error_type):
        if self.error_output==False:
            return -1
        else:
            from fbprophet.diagnostics import cross_validation
            from fbprophet.diagnostics import performance_metrics

            df_cv = cross_validation(m, cutoffs = self.cutoffs, period='30 days', horizon = '30 days', parallel = "processes")
            df_p = performance_metrics(df_cv, rolling_window = 1)
            try:
                return df_p[error_type].values[0]
            except:
                return df_p['mdape'].values[0]
