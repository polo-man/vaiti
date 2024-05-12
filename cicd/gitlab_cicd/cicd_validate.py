import os
import gitlab
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN = 'ВАШ ТОКЕН Gitlab'
# Стандартные переменные среды Gitlab
# см. https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
SERVER = os.environ['CI_SERVER_URL']
COMMIT_SHA = os.environ['CI_COMMIT_SHA']
PROJECT_ID = os.environ['CI_PROJECT_ID']
BRANCH = os.environ['CI_COMMIT_BRANCH']
# Определенная нами переменная среды
BQ_CRED = os.environ['BQ_CRED'] 

# Некоторые символы не могут содержаться в маскируемой переменной, 
# т.ч. содержащийся в кредах сервисного аккаунта Гугл символ \
# Поэтому мы предварительно заменили перевод строки \n на @
# а теперь производим обратную замену
private_key = BQ_CRED.replace('@', '\n')
private_key = '-----BEGIN PRIVATE KEY-----\n' + private_key + '==\n-----END PRIVATE KEY-----\n'

service_account_info = { # реквизиты из JSON-файла с ключом к сервисному аккаунту BQ
  "type": "service_account",
  "project_id": "ID проекта в BQ",
  "private_key_id": "****",
  "private_key": private_key,
  "client_email": "****",
  "client_id": "****",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "****"
}

# Берем текущую ревизию и список измененных файлов
gl = gitlab.Gitlab(SERVER, private_token=TOKEN)
project = gl.projects.get(PROJECT_ID)
commit = project.commits.get(COMMIT_SHA)
changed_files = commit.diff()


# Цикл по всем измененным файлам
for changed_file in changed_files:
    if changed_file['deleted_file'] is True:
        continue #удаленные пропускаем

    file_path = changed_file['new_path']
    print('===')
    print(file_path)
    dest = file_path.split('/') # путь к файлу в репозитории
    print('dest:', dest)

    if dest[0]!='Projects': # берем только файлы из проектов BQ
        print("File is not in 'Projects' directory. Skipped.")
        continue
    if file_path[-4:]!='.sql':
        print('Non-SQL file. Skipped.')
        continue # не SQL файлы пропускаем

    commit_project = dest[1]
    commit_dataset = dest[2]
    commit_table = dest[4][:-4].lower()

    # Берем содержимое файла
    with open(file_path, encoding="utf-8") as f:
        file_content = f.read()
        #print(file_content)
        
    # берем только представления
    if file_content[:22] != "CREATE OR REPLACE VIEW": # Первая строка должна начинаться с этой команды
        print("Not a view or 'CREATE OR REPLACE VIEW' missed. Skipped.")
        continue
    
    # Устанавливаем соединение с BQ
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(commit_project, credentials)
    # Запускаем "холостой" запрос в BQ, чтобы проверить, запустится ли он
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, maximum_bytes_billed=0)
    query_result = client.query(file_content, job_config=job_config)

    # Проверки
    print('Query result: ', query_result.state)    
    if query_result.state != 'DONE':
        print('--> Query failed. Skipped')
        continue
    if query_result.destination.project != commit_project:
        print('project in file path: ', commit_project)
        print('project in query: ', query_result.destination.project)
        print('--> Wrong project. Skipped')
        continue
    if query_result.destination.dataset_id != commit_dataset and not (query_result.destination.dataset_id == 'elama' and commit_dataset == 'elama-small'):
        print('dataset in file path: ', commit_dataset)
        print('dataset in query: ', query_result.destination.dataset_id)
        print('--> Wrong dataset. Skipped')
        continue
    if query_result.destination.table_id.lower() != commit_table.lower():
        print('table in file path: ', commit_table)
        print('table in query: ', query_result.destination.table_id)
        print('--> Wrong table. Skipped')
        continue

    # Запускаем боевой запрос в BQ для записи представления
    work_query = client.query(file_content)
    print(work_query.state)
    print('---')


print('Done.')
    
