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
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sqlquery
        )
        
        self.log.info(f"Insert data into fact table {self.table}")
        redshift.run(formatted_sql)