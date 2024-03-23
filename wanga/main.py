from flask import Flask, request
import sys # Подключаем библиотеку для работы с системными адресами
sys.path.append('ПУТЬ К ФАЙЛУ do_classes.py') # Указываем адрес модуля основных классов
from wanga_classes import getForecast # Используемые функции
import pandas as pd 
import json

app = Flask(__name__)

@app.route('/f', methods = ['GET', 'POST', 'DELETE'])
def f():

    if request.method == 'POST':
        # Получаем данные из POST - запоса
        try:
            received_json = request.form["dataframe"] # Получаем датафрейм
            df = pd.read_json(received_json) # Переводим json в DataFrame
        except:
            df = None

        try: # Проверям, пришёл ли идентификатор задачи прогнозирования
            forecastMarker = request.form["forecastMarker"]
        except:
            forecastMarker = None

        try: # Проверям, пришёл ли идентификатор клиента
            sender = request.form["sender"]
        except:
            sender = None

        try: # Проверям, пришёл ли sql-запрос для самотоятельной загрузки набора данных
            sql_query = request.form["sql_query"]
        except:
            sql_query = None
        
        try: # Проверям, пришёл ли период прогнозирования
            forecasting_period = int(request.form["forecasting_period"])
        except:
            forecasting_period = 365

        try: # Проверям, пришла ли дата после которой начинается прогнозирование
            fake_yesterday = (request.form["fake_yesterday"])
        except:
            fake_yesterday = None

        try: # Проверям, пришёл ли флаг использования гиперпараметров
            use_hypo = (request.form["use_hypo"])
        except:
            use_hypo = True

        try: # Иногда json конвертится в микросекунды, а иногда нет
            df['ds'] = pd.to_datetime(df['ds'], unit='ms') # Переводим время в микросекундах в формат даты
        except:
            pass

        try:
            error_output = request.form['error_output']
        except:
            error_output = False
    
        # Получаем экземпляр класса с прогнозом
        fc = getForecast(
            df, 
            sql_query = sql_query,  
            forecastMarker = forecastMarker, 
            sender = sender, 
            forecasting_period = forecasting_period,
            fake_yesterday = fake_yesterday,
            use_hypo = use_hypo,
            error_output = error_output) 

        jsn = json.loads(fc.forecast.to_json())

        jsn['error_rate'] = fc.error_rate
        jsn['isSpecialParametrs'] = fc.isSpecialParametrs
        jsn['parametrs_in_model'] = fc.parametrs_in_model
        jsn['regressors_in_model'] = fc.regressors_in_model
        jsn['forecasting_period'] = fc.forecasting_period
        
        return json.dumps(jsn)

    return 'eLama forcasting center <br/>Необходимо отправить POST запрос!'

if __name__ == '__main__':
   app.run(host='0.0.0.0')
