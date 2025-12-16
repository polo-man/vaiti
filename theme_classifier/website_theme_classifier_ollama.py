import json
import requests
from typing import Tuple
import ollama
import pandas as pd
import time
from clickhouse_driver import Client
from env import YANDEX_TOKEN, CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
from bs4 import BeautifulSoup
import re

models = ollama.list()
#print(models) # Проверим, подгрузились ли модели

# Подключимся к Кликхаусу, чтобы забрать справочник тематик
client = Client(
    host=CLICKHOUSE_HOST, #продовский
    port=9000,
    user=CLICKHOUSE_USER, 
    password=CLICKHOUSE_PASSWORD
)
query = "SELECT DISTINCT industry, subindustry FROM datamart.themes_dictionary ORDER BY 1,2"
result = client.execute(query)

# создаём пустой словарь для хранения результата
industries_dict = {}
# перебираем строки результата запроса
for row in result:
    industry = row[0]  # industry находится в первом столбце
    subindustry = row[1]  # subindustry находится во втором столбце
    # если industry ещё не в словаре, добавляем её с пустым списком subindustry
    if industry not in industries_dict:
        industries_dict[industry] = []
    # добавляем subindustry в список соответствующей industry
    industries_dict[industry].append(subindustry)

# выводим полученный словарь
#print(industries_dict)
industries_dict = [{"Категория": cat, "Подкатегория": subcat} for cat, subcat in industries_dict.items()]

df = pd.DataFrame(result, columns=['industry', 'subindustry'])
print(df)


def fetch_content(url):
    """
    Получает контент с веб-сайта с обработкой ошибок и структурированием по важности.
    """
    # Нормализуем URL
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        # Создаем структуру для хранения контента с разными приоритетами
        content_structure = {
            'meta_keywords': '',
            'meta_description': '',
            'title': '',
            'h1': '',
            'h2_h3': '',
            'first_paragraphs': '',
            'main_content': ''
        }
        
        # Извлечение meta keywords (наивысший приоритет)
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            content_structure['meta_keywords'] = meta_keywords.get('content', '')
        
        # Извлечение meta description (высокий приоритет)
        meta_description = soup.find('meta', attrs={'name': 'description'})
        if meta_description and meta_description.get('content'):
            content_structure['meta_description'] = meta_description.get('content', '')
        
        # Извлечение заголовка страницы (высокий приоритет)
        title = soup.find('title')
        if title:
            content_structure['title'] = title.get_text()
        
        # Извлечение H1 (высокий приоритет)
        h1_tags = soup.find_all('h1')
        if h1_tags:
            content_structure['h1'] = ' '.join([h.get_text() for h in h1_tags])
        
        # Извлечение H2, H3 (высокий приоритет)
        h2_h3_tags = soup.find_all(['h2', 'h3'])
        if h2_h3_tags:
            content_structure['h2_h3'] = ' '.join([h.get_text() for h in h2_h3_tags])
        
        # Очистка от скриптов и стилей перед извлечением основного контента
        for element in soup(['script', 'style', 'footer', 'header']): #, 'nav']):
            element.extract()
        
        # Извлечение первых параграфов (средний приоритет)
        paragraphs = soup.find_all('p')
        if paragraphs:
            first_paragraphs = paragraphs[:min(5, len(paragraphs))]  # Первые 5 параграфов или меньше
            content_structure['first_paragraphs'] = ' '.join([p.get_text() for p in first_paragraphs])
        
        # Извлечение основного контента (базовый приоритет)
        text_elements = soup.find_all(['p', 'div', 'span', 'li'])
        content_structure['main_content'] = ' '.join([elem.get_text() for elem in text_elements])
        
        # Очистка текста от лишних пробелов и символов для всех полей
        for key in content_structure:
            content_structure[key] = re.sub(r'\s+', ' ', content_structure[key]).strip()
        
        # Объединение всего контента для обратной совместимости
        all_content = []
        for key, value in content_structure.items():
            if value:
                all_content.append(value)
        
        content = ' '.join(all_content)

        #print('self.content_structure: \n', self.content_structure)
        return content_structure, content
    
    except requests.RequestException as e:
        #print(f"Ошибка при запросе к {url}: {e}")
        return False

# Запомним время старта для исследования длительности исполнения
start_time = time.perf_counter()


class KeywordCategorizer:
    def __init__(self, model_name):
        """
        Инициализация категоризатора с использованием Ollama
        Args: model_name: Название модели в Ollama (например, 'llama3.2:3b', 'mistral', 'gemma2:2b')
        """
        self.model_name = model_name
        # Предзагрузка модели
        ollama.generate(model=self.model_name, prompt="", keep_alive=-1)
    
    def categorize_with_ollama(self, prompt) -> Tuple[str, str]:
        """
        Категоризация с использованием Ollama API
        """
        try:
            # Вызов модели через Ollama
            response = ollama.chat(
                model=self.model_name,
                messages=[
                    {
                        'role': 'system',
                        'content': 'Ты эксперт по категоризации сайтов.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                options={
                    'temperature': 0.1,  # Низкая температура для более стабильных результатов
                    'top_p': 0.9,
                    'seed': 42,          # Для стабильной воспроизводимости результата
                }
            )
            # Извлекаем текст ответа
            response_text = response['message']['content']

            return response_text
            
            # Для дополнительных проверок - опционально. 
            # Парсим JSON из ответа
            # Пытаемся найти JSON в тексте, если модель добавила лишний текст
            import re
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                return result.get('category', 'Не определено'), result.get('subcategory', 'Не определено')
            else:
                raise ValueError("JSON не найден в ответе")
                
        except Exception as e:
            print(f"Ошибка при работе с Ollama: {e}")
            return false #self.fallback_categorization(keywords)

# Внесите несколько сайтов через запятую. Тематики сайтов должны входить в те, что есть в справочнике
domains = ['https://site1.ru',
'https://site2.ru']

# Инициализация пустого датафрейма с нужными колонками
df_result = pd.DataFrame(columns=['domain', 'theme', 'subtheme'])

domain_start_time = start_time
for domain in domains:
    content_structure, _ = fetch_content(domain)
    print(domain)
    
    ''' Промпт для LLM, включающий в себя: 
        * структурированный контент сайта
        * наш справочник тематик
        * чёткую задачу
        * строгий формат вывода
        * дополнительную угрозу - для повышения качества результата
    '''
    prompt = f"""<context>Дан сайт {domain} и его данные:
    <title>{content_structure['title']}</title>
    <meta keywords>{content_structure['meta_keywords']}</meta keywords>
    <meta description>{content_structure['meta_description']}</meta description>
    <h1>{content_structure['h1']}</h1>
    <content>{content_structure['first_paragraphs']} {content_structure['main_content'][:800]}</content>
    </context>
    
    <task>Выбери наиболее подходящую категорию и только одну наиболее подходящую подкатегорию только из справочника категорий: {industries_dict}.
    </task>
    
    <output_format>
    Отвечай ТОЛЬКО в формате JSON без дополнительного текста.
    Строгий формат ответа:
    {{"категория": "название категории", "подкатегория": "название подкатегории"}}
    </output_format>
    <restrictions>
    НЕ ДОЛЖНО БЫТЬ КАТЕГОРИЙ И ПОДКАТЕГОРИЙ, КОТОРЫХ НЕТ В ЗАДАННОМ СПРАВОЧНИКЕ КАТЕГОРИЙ.   
    За каждую ошибку я буду больно бить тебя током.
    </restrictions>
    """

    # Применяем модель для категоризации сайтов
    categorizer = KeywordCategorizer(model_name="llama3.2:3b") 
    result = categorizer.categorize_with_ollama(prompt)

    print(result)
    result = json.loads(result)
    new_row = pd.DataFrame({
        'domain': [domain],
        'theme': [result['категория']],
        'subtheme': [result['подкатегория']]
    })
    df_result = pd.concat([df_result, new_row], ignore_index=True)

    end_time = time.perf_counter()
    print(f"Время выполнения для домена {domain}: {end_time - domain_start_time } секунд")
    domain_start_time = end_time
    print('-------------------------')

end_time = time.perf_counter()
print(f"Общее время выполнения: {end_time - start_time} секунд")
df_result
