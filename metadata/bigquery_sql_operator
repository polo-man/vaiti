import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.hooks.bigquery_hook import BigQueryHook
import config


class BigquerySqlOperator(BaseOperator):
    """
    Execute sql script. Not returning any data.

    :param bq_project_id: Big Query Project id
    :type bq_project_id: str

    :param sql_script: sql script to execute. NB! Last query should not ends with ';'
    :type: str

    """
    template_fields = ('sql_script', 'params')
    template_ext = ('.sql', '.hql')

    @apply_defaults
    def __init__(self,
                 bq_project_id,
                 sql_script,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.bq_project_id = bq_project_id
        self.sql_script = sql_script

    def execute(self, context):
        
        if config.ENV != 'prod':
            logging.info('not prod env')
            return
        
        bigquery_hook = BigQueryHook(project_id=self.bq_project_id)

        queries = self.sql_script.split(";\n")
        for query in queries:
            bigquery_hook.run(query)
