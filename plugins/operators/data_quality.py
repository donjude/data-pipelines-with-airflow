from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_null_checks=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.sql_null_checks = sql_null_checks
        self.expected_results = expected_results


    def execute(self, context):
        redshift = PostgresHook(post_conn_id=self.redshift_conn_id)
        
        if len(self.sql_null_checks) != len(self.expected_results):
            raise ValueError('lengths for test and expected results do not match')


        for i in range(len(self.sql_null_checks)):
            result = redshift.get_first(self.sql_null_checks[i])
            if result[0] != self.expected_results[i]:
                raise ValueError('data quality test {} failed'.format(i))
            else:
                self.log.info("data quality test {} passed".format(i))