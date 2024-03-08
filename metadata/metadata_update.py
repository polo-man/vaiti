import logging
import re
from common.hooks.bigquery_hook import BigQueryHook
import datetime
import config

# update_description=False - дескрипшен остается как в исходной схеме
# update_description=True - дескрипшен устанавливается как в гугл щите

# Функция отправки запроса в BigQuery и получения результата
def get_BigQuery_results(project_id:str, sql:str ):
   query = sql
   logging.info(sql)
   bigquery = BigQueryHook(project_id=project_id)
   client = bigquery.get_client()
   query_job = client.query(query)
   results = query_job.result()
   return results

# Функция обновления метаданных одной таблицы
def update_schema(project, dataset, table_name, update_description=False):
    if not update_description:
        logging.info("update_description is False - exiting")
        return 0
    table_meta = re.sub(r'_\d{8}$','_', table_name) # для датированных таблиц отсечем дату
    table_id_meta = project + "." + dataset + "." + table_meta
    table_id = project + "." + dataset + "." + table_name

    _bigquery = BigQueryHook(project_id=project)

    credentials_read_metadata = config.BQ_CREDENTIALS[PROJECT_ID_METADATA]
    credentials_bq_read_metadata = service_account.Credentials.from_service_account_info(json.loads(credentials_read_metadata))

    print("table_id: ", table_id)
    print("table_id_meta: ", table_id_meta)

    # Construct a BigQuery client object.
    client = _bigquery.get_client()
 
    try:
        dataset_ref = client.get_dataset(dataset)
        table = client.get_table(dataset_ref.table(table_name))
    except:
        print("Can't find table ", table_id)
        return
    original_schema = table.schema
    original_desc = table.description
    print(f"original_desc={original_desc}")
    sql = f"""SELECT CONCAT(project, '.', dataset, '.', table_name) AS table,
            field, field_description as description, tag_id
        FROM `{PROJECT_ID_METADATA}.{DATASET_METADATA}.table_schema`
        WHERE project IS NOT NULL
        GROUP BY 1,2,3,4
        HAVING table = '{table_id_meta}'
        """
    print(sql)
    df = pd.read_gbq(sql, project_id=PROJECT_ID_METADATA, credentials=credentials_bq_read_metadata)
    print(df)
    
    sql = f"""SELECT CONCAT(project, '.', dataset, '.', table) AS table,
            option, option_name, option_value
        FROM `{PROJECT_ID_METADATA}.{DATASET_METADATA}.table_metadata`
        WHERE project IS NOT NULL
        GROUP BY 1,2,3,4
        HAVING table = '{table_id_meta}'
        """
    df2 = pd.read_gbq(sql, project_id=PROJECT_ID_METADATA, credentials=credentials_bq_read_metadata)
 
    # Поля таблицы
    if len(df[df.table==table_id_meta])>0:
        schema=[]
        for f in original_schema:
            print(6.1, f.name, f)
            desc=''
            df_desc = df[(df.table==table_id_meta)&(df.field==f.name)]
            if len(df_desc):
                desc = df_desc['description'].values[0]
            if desc:
                schema.append(bigquery.SchemaField(\
                    name=f.name, field_type=f.field_type, mode=f.mode,\
                    description=(desc if update_description else f.description)))
            else:
                schema.append(bigquery.SchemaField(\
                    name=f.name, field_type=f.field_type, mode=f.mode,\
                    description=( f.description)))
            print(f'schema={schema}')
            
        table.schema = schema
        print(table.schema)
        
        # Make an API request
        table = client.update_table(table, ["schema"])  
        
    print(table.schema)
    df_metadata = df2[df2.table==table_id_meta]
    if len(df_metadata)>0:
        # Описание таблицы    
        print(df_metadata[df_metadata.option=='description'].info())
        new_table_description = df_metadata[df_metadata.option=='description']['option_value'].values[0]
        print(new_table_description)

        table.description = new_table_description if new_table_description!='' else original_desc
        print( table.description)
        table = client.update_table(table, ["description"])  # Make an API request.
        # Метки таблицы
        new_labels = {}
        for index, row in df2[df2.option=='label'].iterrows():
            new_labels[row['option_name']] = row['option_value']
        print(new_labels)
        table.labels = new_labels #if len(new_labels)>0 else original_labels
               
        if len(new_labels) > 0:
            table = client.update_table(table, ["labels"])  # Make an API request.



# Функция прохода по списку витрин
def update_tables(sql_metadata, project_id, update_description=False, update_policy=False):
    df = get_BigQuery_results(project_id, sql_metadata)
    df_tables_metadata = df.to_dataframe()    

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
            
            # если на конце нет нижнего подчеркивания, то обновляем только указанную таблицу и не трогаем датированные (речь об таблицах вида table_name_20240310, где цифры - дата обновления витрины)
            if m_table[-1] != '_':
                logging.info(f"refresh only table: {m_table}")
                try:
                    table_info = client.get_table(f"{m_project_id}.{m_dataset}.{m_table}")
                    last_modified_date = table_info.modified
                    if last_modified_date.date() == today:
                        logging.info(f"refresh metadata project_id: {m_project_id}, dataset: {m_dataset}, table: {m_table}")
                        update_schema(m_project_id, m_dataset, m_table, update_description=update_description)
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
                    # Пишем метаданные только сегодня обновленных витрин, чтобы даты обновления данных и метаданных в интерфейсе витрин совпадали
                    if last_modified_date.date() == today:
                        logging.info(f"refresh metadata project_id: {m_project_id}, dataset: {m_dataset}, table: {res_table}")
                        update_schema(m_project_id, m_dataset, res_table, update_description=update_description)
                    else:
                        logging.info(f"Skipping metadata update for {m_project_id}.{m_dataset}.{res_table} as it was not modified today")    
                except Exception as err:
                    list_errors.append(f"{m_project_id}.{m_dataset}.{res_table}")
                    logging.error(f"ERROR in update_metadata - {m_project_id}.{m_dataset}.{res_table}\n{str(err)}")

    if list_errors:
        logging.info(list_errors)
