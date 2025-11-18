import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import math
from clickhouse_driver import Client
import numpy as np
import ftfy # библиотека для исправления кодировки
# Креды для подключения к Clickhouse
from env import YANDEX_TOKEN, CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD


class WebsiteAnalyzer:
    """
    Класс для определения тематики сайта
    """
    def __init__(self, url, print_flag):
        self.url = url
        self.content = ""
        self.filtered_tokens = []
        self.keywords = []
        self.print_flag = print_flag # флаг вывода полной информации о процессе
        
    def normalize_url(self):
        """
        Нормализует URL, добавляя протокол, если его нет.
        """
        url = self.url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url
        
    def fetch_content(self, max_retries=3, delay=1):
        """
        Получает контент с веб-сайта с обработкой ошибок и структурированием по важности.
        """
        # Нормализуем URL
        url = self.normalize_url()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        for attempt in range(max_retries):
            try:
                #print(f"Получение контента с {url}, попытка {attempt+1}/{max_retries}")
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()

                #print("Подключились к сайту, анализируем структуру")
                soup = BeautifulSoup(response.text, 'html.parser')
                # Сохраняем объект soup для дальнейшего использования
                self.soup = soup
                # Создаем структуру для хранения контента с разными приоритетами
                self.content_structure = {
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
                    self.content_structure['meta_keywords'] = meta_keywords.get('content', '')
                
                # Извлечение meta description (высокий приоритет)
                meta_description = soup.find('meta', attrs={'name': 'description'})
                if meta_description and meta_description.get('content'):
                    self.content_structure['meta_description'] = meta_description.get('content', '')
                
                # Извлечение заголовка страницы (высокий приоритет)
                title = soup.find('title')
                if title:
                    self.content_structure['title'] = title.get_text()
                
                # Извлечение H1 (высокий приоритет)
                h1_tags = soup.find_all('h1')
                if h1_tags:
                    self.content_structure['h1'] = ' '.join([h.get_text() for h in h1_tags])
                
                # Извлечение H2, H3 (высокий приоритет)
                h2_h3_tags = soup.find_all(['h2', 'h3'])
                if h2_h3_tags:
                    self.content_structure['h2_h3'] = ' '.join([h.get_text() for h in h2_h3_tags])
                
                # Очистка от скриптов и стилей перед извлечением основного контента
                for element in soup(['script', 'style', 'footer', 'header']): #, 'nav']):
                    element.extract()
                
                # Извлечение первых параграфов (средний приоритет)
                paragraphs = soup.find_all('p')
                if paragraphs:
                    first_paragraphs = paragraphs[:min(5, len(paragraphs))]  # Первые 5 параграфов или меньше
                    self.content_structure['first_paragraphs'] = ' '.join([p.get_text() for p in first_paragraphs])
                
                # Извлечение основного контента (базовый приоритет)
                text_elements = soup.find_all(['p', 'div', 'span', 'li'])
                self.content_structure['main_content'] = ' '.join([elem.get_text() for elem in text_elements])
                
                # Очистка текста от лишних пробелов и символов для всех полей
                for key in self.content_structure:
                    self.content_structure[key] = re.sub(r'\s+', ' ', self.content_structure[key]).strip()
                
                # Объединение всего контента для обратной совместимости
                all_content = []
                for key, value in self.content_structure.items():
                    if value:
                        all_content.append(value)
                
                self.content = ' '.join(all_content)

                return True
            
            except requests.RequestException as e:
                print(f"Ошибка при запросе к {url}: {e}")
                return False
                #if attempt < max_retries - 1:
                #    print(f"Повторная попытка через {delay} секунд...")
                #    #time.sleep(delay)
                #else:
                #    print(f"Не удалось получить контент с {url} после {max_retries} попыток.")
                #    return False
    
    def process_text(self, language='auto'):
        """
        Обрабатывает текст и выделяет ключевые слова с ранжированием по релевантности.
        Выделяет только существительные и прилагательные и приводит их к базовой форме.
        """
        if not hasattr(self, 'content') or not self.content:
            #print(f"Нет контента для обработки с {self.url}")
            return False

        # Проверка и исправление кодировки
        self.content = ftfy.fix_text(self.content)

        # Определение языка, если установлено auto
        if language == 'auto':
            if any([re.search('[а-яА-Я]', char) for char in self.content[:1000]]):
                language = 'russian'
            else:
                return False # Алгоритм определяет тематики только для русскоязычных сайтов
        
        # Импорты Natasha для русского языка
        if language == 'russian':
            from natasha import (
                Segmenter,
                MorphVocab,
                NewsEmbedding,
                NewsMorphTagger,
                Doc
            )
            # Инициализация компонентов Natasha
            segmenter = Segmenter()
            morph_vocab = MorphVocab()
            emb = NewsEmbedding()
            morph_tagger = NewsMorphTagger(emb)
        
        # Функция для токенизации, фильтрации и лемматизации
        def process_segment(text, source_priority):
            filtered_lemmas = []
            # Обработка русского текста с Natasha
            doc = Doc(text)
            doc.segment(segmenter)
            doc.tag_morph(morph_tagger)
            
            for token in doc.tokens:
                # Разрешаем буквы, дефисы и подчеркивания
                if (len(token.text) <= 2 or 
                    not re.match(r'^[а-яёa-z\-]+$', token.text.lower())):
                    continue
                
                # Получаем морфологические характеристики
                pos = token.pos
                
                # Фильтрация только существительных (NOUN) и прилагательных (ADJ)
                if pos not in ['NOUN', 'ADJ']:
                    continue
                
                # Получаем исходную форму слова с помощью morph_vocab
                parsed_token = morph_vocab.parse(token.text)[0]
                normalized_token = parsed_token.normal
                
                try:
                    # Для существительных: именительный падеж, единственное число
                    if pos == 'NOUN':
                        # Проверяем, может ли слово иметь единственное число
                        if 'Number=Plur' in token.feats and not 'Pluralia_tantum' in parsed_token.tag:
                            # Пытаемся привести к единственному числу
                            inflected = morph_vocab.parse(token.text)[0].inflect({'sing', 'nomn'})
                            if inflected:
                                normalized_token = inflected.word
                        else:
                            # Приводим только к именительному падежу
                            inflected = morph_vocab.parse(token.text)[0].inflect({'nomn'})
                            if inflected:
                                normalized_token = inflected.word
                    
                    # Для прилагательных: именительный падеж, единственное число, мужской род
                    elif pos == 'ADJ':
                        inflected = morph_vocab.parse(token.text)[0].inflect({'sing', 'nomn', 'masc'})
                        if inflected:
                            normalized_token = inflected.word
                except:
                    # В случае ошибки используем исходную форму без вывода сообщения
                    pass
                
                # Переводим в нижний регистр для единообразия
                normalized_token = normalized_token.lower()
                
                filtered_lemmas.append((normalized_token, source_priority))
            
            return filtered_lemmas
        
        # Обработка разных сегментов контента с учетом их приоритета
        priority_map = {
            'meta_keywords': 10,    # Наивысший приоритет
            'meta_description': 9,
            'title': 9,
            'h1': 8,
            'h2_h3': 7,
            'first_paragraphs': 6,
            'main_content': 5       # Базовый приоритет
        }
        
        all_lemmas = []
        
        # Обрабатываем каждый сегмент контента
        if hasattr(self, 'content_structure'):
            for segment_name, priority in priority_map.items():
                if segment_name in self.content_structure and self.content_structure[segment_name]:
                    self.content_structure[segment_name] = ftfy.fix_text(self.content_structure[segment_name])
                    segment_lemmas = process_segment(self.content_structure[segment_name], priority)
                    all_lemmas.extend(segment_lemmas)
        else:
            # Если структура контента не определена, обрабатываем весь контент с базовым приоритетом
            all_lemmas = process_segment(self.content, 5)
        
        # Подсчет TF (term frequency) для каждой леммы
        lemma_counts = {}
        for lemma, _ in all_lemmas:
            lemma_counts[lemma] = lemma_counts.get(lemma, 0) + 1

        # Расчет TF-IDF и итогового рейтинга релевантности
        total_lemmas = len(all_lemmas)
        lemma_scores = {}
        
        if total_lemmas > 0:
            for lemma, priority in all_lemmas:
                if lemma not in lemma_scores:
                    # TF компонент
                    tf = lemma_counts[lemma] / total_lemmas
                    
                    # Имитация IDF (чем реже слово, тем выше его значимость)
                    max_count = max(lemma_counts.values())
                    idf = math.log(max_count / (lemma_counts[lemma] + 1) + 1)
                    
                    # Базовый TF-IDF счет
                    tfidf_score = tf * idf * 3  # Масштабируем TF-IDF для сравнимости с приоритетом
                    
                    # Средний приоритет из всех вхождений слова
                    priority_sum = sum([p for l, p in all_lemmas if l == lemma])
                    priority_count = sum([1 for l, p in all_lemmas if l == lemma])
                    avg_priority = priority_sum / priority_count
                    
                    # Итоговый рейтинг: комбинация TF-IDF и приоритета источника
                    # Нормализуем до шкалы 1-10
                    final_score = min(round((tfidf_score + avg_priority) / 2), 10)
                    
                    lemma_scores[lemma] = final_score
        
        # Сортировка и выбор топ слов
        sorted_lemmas = sorted(lemma_scores.items(), key=lambda x: x[1], reverse=True)

        # кол-во слов: 70
        self.keywords_with_relevance = sorted_lemmas[:70]
        
        # Для обратной совместимости сохраняем список ключевых слов без рейтинга
        self.keywords = [lemma for lemma, _ in self.keywords_with_relevance]
        
        #print(f"Обработано {total_lemmas} лемм, выделено {len(self.keywords)} ключевых слов")
        return True


    def determine_theme_and_subtheme(self, keyword_dict):
        """
        Определяет тематику и подтематику сайта с учетом релевантности ключевых слов.
        """
        if not hasattr(self, 'keywords_with_relevance') or not self.keywords_with_relevance:
            #print(f"Нет ключевых слов для анализа с {self.url}")
            return None, None, 0, None
        
        if self.print_flag:
            print('Ключевые слова сайта с релевантностью:', self.keywords_with_relevance)
        
        # Создаем словари для быстрого доступа
        site_keyword_dict = {kw: rel for kw, rel in self.keywords_with_relevance}
        
        # Создаем словарь для хранения взвешенных совпадений
        matches = {}
        total_score = 0
         
        # Для каждой тематики и подтематики подсчитываем взвешенный счет совпадений
        for theme, subthemes in keyword_dict.items():
            matches[theme] = {}
            
            for subtheme, keywords_relevance_dict in subthemes.items():
                # Подсчет взвешенных совпадений
                weighted_score = 0
                match_count = 0
                match_details = []
                strong_keywords = 0
                
                for site_kw, site_rel in self.keywords_with_relevance:
                    if site_kw in keywords_relevance_dict:
                        theme_rel = keywords_relevance_dict[site_kw]                        
                        # Вес совпадения - произведение релевантностей
                        match_weight = site_rel * theme_rel                        
                        weighted_score += match_weight
                        match_count += 1
                        if theme_rel >= 8: strong_keywords += 1
                        match_details.append((site_kw, site_rel, theme_rel, match_weight))
                        total_score += match_weight
                
                # Сохраняем только если есть хотя бы одно совпадение
                if match_count > 0:
                    matches[theme][subtheme] = {
                        'weighted_score': weighted_score,
                        'match_count': match_count,
                        'details': match_details,
                        'strong_keywords': strong_keywords
                    }
                    
                    if self.print_flag:
                        print(f'Тематика: {theme}; Подтематика: {subtheme}')
                        print(f'Совпадений: {match_count}, Сильных совпадений: {strong_keywords}, Взвешенный счет: {weighted_score}')
                        print('Совпавшие слова:')
                        for kw, site_rel, theme_rel, weight in match_details:
                            print(f'  - {kw}: вес сайта {site_rel}, вес тематики {theme_rel}, итоговый вес {weight:.2f}')
                            
                        print('---------------------')
                        
        # Добавляем поле weighted_score_percent
        for theme in matches:
            for subtheme in matches[theme]:
                weighted_score = matches[theme][subtheme]['weighted_score']
                weighted_score_percent = weighted_score/total_score
                matches[theme][subtheme]['weighted_score_percent'] = weighted_score_percent
        print('total_score: ', total_score)
        # Находим тематику и подтематику с наибольшим взвешенным счетом
        best_theme = None
        best_subtheme = None
        max_weighted_score = 0
        best_matched_keywords = []  # Список совпавших ключевых слов
        
        for theme, subthemes in matches.items():
            for subtheme, scores in subthemes.items():
                if scores['weighted_score'] > max_weighted_score:
                    max_weighted_score = scores['weighted_score']
                    best_theme = theme
                    best_subtheme = subtheme
                    # Сохраняем совпавшие ключевые слова
                    best_matched_keywords = [detail[0] for detail in scores['details']]  # Только ключевые слова

        # Преобразование словаря в список строк для DataFrame
        rows = []
        for theme, subthemes in matches.items():
            for subtheme, data in subthemes.items():
                rows.append({
                    'weighted_score': data.get('weighted_score', 0),
                    'match_count': data.get('match_count', 0),
                    'strong_keywords': data.get('strong_keywords', 0),
                    'weighted_score_percent': data.get('weighted_score_percent', 0.0)
                })
        df_matches = pd.DataFrame(rows)
        df_matches = df_matches.sort_values(
            by=['weighted_score', 'match_count', 'strong_keywords'], 
            ascending=False
        )

        ######## Рассчитываем уверенность
        c = [0.1, 0.3, 0.5, 7] # веса факторов
        # Фактор: Количество совпавших ключей (K)
        def get_k(match_count):
            """Функция для расчета K"""
            if match_count < 3: # если совпавших ключей <3
                return 0.1      # значение фактора K самое низкое
            elif match_count == 3:
                return 0.5
            elif match_count in [4, 5]:
                return 0.8
            elif match_count > 5:
                return 1.0
            else:
                return None
        df_matches['K_'] = df_matches['match_count'].apply(get_k)
        K = df_matches['K_'].iloc[0]
        # Фактор: Нормализованное количество сильных ключей (с весом 8+)
        Kw = df_matches['strong_keywords'].iloc[0] / df_matches['strong_keywords'].max()
        # Фактор: доля суммы баллов
        b_ = df_matches['weighted_score_percent'].iloc[0]
        # Фактор: разница суммы со следующей тематикой
        d = b_ - df_matches['weighted_score_percent'].iloc[1]
        print(f'K = {K}, Kw = {Kw}, b_ = {b_}, d = {d}')
        confidence = K*c[0] + Kw*c[1] + b_*c[2] + d*c[3]
        if confidence>1: confidence = 1
        print('confidence: ', confidence)
       
        return best_theme, best_subtheme, confidence, best_matched_keywords


    def extract_domain(self):
        """
        Извлекает домен из URL.
        """
        try:
            parsed_url = urlparse(self.normalize_url())
            domain = parsed_url.netloc
            return domain
        except:
            return self.url
    
    def analyze(self, keyword_dict):
        """
        Выполняет полный анализ сайта: загрузка контента, обработка, определение тематики.
        Args:
            keyword_dict: Словарь вида {тематика: {подтематика: [ключевые слова]}}
        Returns:
            dict: Результаты анализа
        """
        # Загружаем контент
        if not self.fetch_content():
            return {
                'url': self.url,
                'domain': self.extract_domain(),
                'theme': None,
                'subtheme': None,
                'confidence': 0,
                'matched_keywords': []
            }
        
        # Обрабатываем текст
        if not self.process_text():
            return {
                'url': self.url,
                'domain': self.extract_domain(),
                'theme': None,
                'subtheme': None,
                'confidence': 0,
                'matched_keywords': []
            }
        
        # Определяем тематику и подтематику
        theme, subtheme, confidence, matched_keywords = self.determine_theme_and_subtheme(keyword_dict)
        if self.print_flag:
            print(self.url, ": ", theme, subtheme)
        
        # Формируем результат
        result = {
            'url': self.url,
            'domain': self.extract_domain(),
            'theme': theme,
            'subtheme': subtheme,
            'confidence': confidence,
            'matched_keywords': matched_keywords,
            'top_keywords': self.keywords[:50]  # Включаем топ-50 ключевых слов для проверки
        }
        
        return result


def analyze_websites_batch(urls, keyword_dict, print_flag=False, max_workers=5):
    """
    Анализирует пакет веб-сайтов параллельно.
    Args:
        urls (list): Список URL
        keyword_dict (dict): Словарь ключевых слов
        max_workers (int): Максимальное количество параллельных задач
    Returns:
        list: Список результатов анализа
    """
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {}
        
        # Отправляем задачи на анализ
        for url in urls:
            analyzer = WebsiteAnalyzer(url, print_flag)
            future = executor.submit(analyzer.analyze, keyword_dict)
            future_to_url[future] = url
        
        # Собираем результаты
        for future in future_to_url:
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
                if print_flag: print(f"Обработан сайт {url}, тематика: {result['theme']}, подтематика: {result['subtheme']}")
            except Exception as e:
                if print_flag: print(f"Ошибка при обработке {url}: {e}")
                results.append({
                    'url': url,
                    'domain': url,
                    'theme': None,
                    'subtheme': None,
                    'confidence': 0,
                    #'status': 'error',
                    #'message': str(e)
                })
    
    return results
    

# Клиент для подключения к Кликхаусу
client = Client(
    host=CLICKHOUSE_HOST,
    port=9000,
    user=CLICKHOUSE_USER, 
    password=CLICKHOUSE_PASSWORD
)

# Забираем справочник соответствия тематик и ключевых слов
query = """SELECT industry, subindustry, keyword, weight 
    FROM datamart.domain_theme_keywords ORDER BY 1,2,4 DESC, 3
    """
result = client.execute(query)

# Создание вложенного словаря
keyword_dict = {}
for row in result:
    industry, subindustry, keyword, weight = row
    
    # Создаем структуру словаря
    if industry not in keyword_dict:
        keyword_dict[industry] = {}
    if subindustry not in keyword_dict[industry]:
        keyword_dict[industry][subindustry] = {}
    
    # Добавляем ключевое слово и его вес
    keyword_dict[industry][subindustry][keyword] = weight

# список сайтов для определения тематик
sites = ['elama.ru', 'vaiti.io']

# вызов функции определения тематик
df_results = analyze_websites_batch(sites, keyword_dict, print_flag=False, max_workers=5)

# Вывод результатов
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
df = pd.DataFrame(df_results)
display(df)