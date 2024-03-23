import json # стандартная библиотека
import requests # стандартная библиотека
 
ENDPOINT = "http://IP виртуальной машины с Вангой/f" # defining the api-endpoint
 
data = {'dataframe' : df.to_json(), # датафрейм с набором данных переводим в json
        'forecastMarker' : '...',  # необязательный параметр - идентификатор задачи прогнозирования
        'sender' : 'jupyter notebook', # необязательный параметр - идентификатор отправителя запроса
        'sql_query' : 'SELECT ... FROM ... WHERE ...' # необязательный параметр - sql-запрос для самостоятельного парсинга системой
        }
 
r = requests.post(url = ENDPOINT, data = data) # Отправка POST-запроса к микросервису
 
j = json.loads( # Разбор ответа, полученного в формате JSON
    r.text
)
 
result = pd.DataFrame({ # Формируем датафрейм с прогнозом
    'ds':j['ds'],
    'y': j['y'],
    'yhat': j['yhat'],
    'iliPlanIliFact': j['iliPlanIliFact'] # Колонка, которая содержит объединённые данные: факт - для прошедших дат и прогноз для будущего
})
 
result['ds'] = pd.to_datetime(result['ds'], unit='ms') # Переводим время в микросекундах в формат даты

