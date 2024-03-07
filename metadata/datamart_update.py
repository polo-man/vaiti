def recache_table(**kwargs):

    sql = f"""
        SELECT *
        FROM `{PROJECT_ID}.others.recache_table_for_pbi_reports_cache` 
        ORDER BY coalesce(launch_order, 999999) 
        """

    df = get_BigQuery_results(PROJECT_ID, sql)
    df = df.to_dataframe()
    failed_cache = []

    date = datetime.today().day # определяем какое сегодня число
    weekday = datetime.isoweekday(datetime.now()) # опеределяем день недели, для обновления витрин в определенённые дни
    df['schedule'].fillna(0, inplace=True) # замена пустых значений на 0, для корректной проверки. 0 - значит ежедневное обновление
    df['month_update'].fillna(0, inplace=True)

    recache = df[(df['schedule']==0) | (df['schedule']==weekday)].copy()
    recache = recache[(recache['month_update']==0) | (recache['month_update']==date)]

    for tabel in recache.index:
        destination_table = f"""{recache.loc[tabel, 'cache_project']}.{recache.loc[tabel, 'cache_dataset']}.{recache.loc[tabel, 'cache_table']}"""
        query_for_recached = f"""
        CREATE OR REPLACE TABLE `{destination_table}` AS 
        SELECT * FROM `{recache.loc[tabel,'source_project']}.{recache.loc[tabel,'source_dataset']}.{recache.loc[tabel,'source_view']}`; 
        """
        try:
            BigQueryHook(project_id=PROJECT_ID).run(query_for_recached)
            logging.info(f"""Витрина {destination_table} успешно обновлена.""")
        except:
            logging.info(f"""Не удалось обновить витрину {destination_table}!""")
            push_custom_message(f"""Не удалось обновить витрину {destination_table}!""")
            failed_cache.append(destination_table)
        
    logging.info(failed_cache)
    if len(failed_cache)>0:
        for table in failed_cache:
            # Запишем не обновившиеся витрины в лог инцидентов
            save_incident(PROJECT_ID, table_name=table, checked_parameter='Витрина отчета',\
                error='Не обновилась')

    return 0
