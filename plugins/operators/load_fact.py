from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """ 

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sqlquery=sqlquery
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sqlquery = sqlquery
        

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
