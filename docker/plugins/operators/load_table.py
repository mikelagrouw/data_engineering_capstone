"""Load table."""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Load table."""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table='',
                 sql='',
                 conn_id='',
                 truncate=False,
                 *args, **kwargs):
        """Initialize."""
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.conn_id = conn_id
        self.truncate = truncate

    def execute(self, context):
        """Execute loading."""
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate:
            redshift.run("DELETE FROM {}".format(self.table))

        sql = "INSERT INTO {} {}".format(self.table, self.sql)
        self.log.info("running sql statement: " + sql)
        redshift.run(sql)
        self.log.info('LoaddimensionOperator completed')
