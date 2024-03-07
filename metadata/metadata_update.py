import logging
import re
from common.hooks.bigquery_hook import BigQueryHook
from common.utils.check_BigQuery_utils import get_dataframe_from_bigquery
from common.utils.bigquery_metadata import update_schema
from common.handlers.slack import push_custom_message
import datetime

# update_policy=False - полиси тэги остаются как в исходной схеме
# update_policy=True - полиси тэги устанавливаются как в гугл щите
# то же самое с дескрипшеном


def update_tables(sql_metadata, project_id, update_description=False, update_policy=False):
    df_tables_metadata = get_dataframe_from_bigquery(sql=sql_metadata, project_id=project_id)

    list_errors = []
    
    today = datetime.date.today()

    bigquery = BigQueryHook(project_id=project_id)
    client = bigquery.get_client()

    df_project_dataset = df_tables_metadata[['project', 'dataset']].drop_duplicates()
    for index, project_dataset in df_project_dataset.iterrows():
        m_project_id = project_dataset['project']
        m_dataset = project_dataset['dataset']
        m_tables = df_tables_metadata[(df_tables_metadata.project == m_project_id) & (df_tables_metadata.dataset == m_dataset)]
        
        try:
            tables_bq_g = client.list_tables(f'{m_project_id}.{m_dataset}')
            tables_bq = [tbl.table_id for tbl in tables_bq_g]
        except Exception as err:
            list_errors.append(f"{m_project_id}.{m_dataset}")
            tables_bq = []
            logging.error(f"ERROR in get list table from bq: {m_project_id}.{m_dataset}\n{str(err)}")

        for index, row in m_tables.iterrows():
            m_table = row['table']
            logging.info(f"get m_table - project_id: {m_project_id}, dataset: {m_dataset}, table: {m_table}")
            
            # если на конце нет нижнего подчеркивания, то обновляем только указанную таблицу и не трогаем датированные
            if m_table[-1] != '_':
                logging.info(f"refresh only table: {m_table}")
                try:
                    table_info = client.get_table(f"{m_project_id}.{m_dataset}.{m_table}")
                    last_modified_date = table_info.modified
                    if last_modified_date.date() == today:
                        logging.info(f"refresh metadata project_id: {m_project_id}, dataset: {m_dataset}, table: {m_table}")
                        update_schema(m_project_id, m_dataset, m_table, update_description=update_description, update_policy=update_policy)
                    else:
                        logging.info(f"Skipping metadata update for {m_project_id}.{m_dataset}.{m_table} as it was not modified today")
                    
                except Exception as err:
                    list_errors.append(f"{m_project_id}.{m_dataset}.{m_table}")
                    logging.error(f"ERROR in update_metadata - {m_project_id}.{m_dataset}.{m_table}\n{str(err)}")
                continue

            # если на конце есть нижнее подчеркивание, то обновляем последнюю датированную таблицу
            logging.info(f"refresh dated table: {m_table}")

            date_tables = list(filter(lambda tbl: bool(re.match(m_table + r"_?(\d{8})?$", tbl)), tables_bq))
            logging.info(f"get list  date_tables project_id: {m_project_id}, dataset: {m_dataset}, date_tables: {date_tables}")
            date_tables.sort()
            if date_tables:
                res_table = date_tables[-1]
                try:
                    table_info = client.get_table(f"{m_project_id}.{m_dataset}.{res_table}")
                    last_modified_date = table_info.modified
                    if last_modified_date.date() == today:
                        logging.info(f"refresh metadata project_id: {m_project_id}, dataset: {m_dataset}, table: {res_table}")
                        update_schema(m_project_id, m_dataset, res_table, update_description=update_description, update_policy=update_policy)
                    else:
                        logging.info(f"Skipping metadata update for {m_project_id}.{m_dataset}.{res_table} as it was not modified today")    
                except Exception as err:
                    list_errors.append(f"{m_project_id}.{m_dataset}.{res_table}")
                    logging.error(f"ERROR in update_metadata - {m_project_id}.{m_dataset}.{res_table}\n{str(err)}")

    if list_errors:
        logging.info(list_errors)
        push_custom_message(f"Не удалось обновить метаданные таблиц: {list_errors}")
