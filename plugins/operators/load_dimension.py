from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sqlquery="",
                 append = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sqlquery = sqlquery
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            self.log.info(f"Remove data from {self.table} dimension table")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sqlquery
        )
        self.log.info(f"Insert data into dimentation {self.table} from staging")
        redshift.run(formatted_sql)
