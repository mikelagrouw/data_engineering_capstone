"""Stage to redshift."""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Stage to redshift."""

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format='',
                 ignore_header='',
                 *args, **kwargs):
        """Initialize."""
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format = format
        self.ignore_header = ignore_header

    def execute(self, context):
        """Execute staging."""
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        sql = """
              copy {table} from 's3://{bucket}/{key}'
              ACCESS_KEY_ID '{key_aws}'
              SECRET_ACCESS_KEY '{secret}'
              format as {format} compupdate off
              {ignore}

        """.format(table=self.table,
                   bucket=self.s3_bucket,
                   key=self.s3_key,
                   key_aws=credentials.access_key,
                   secret=credentials.secret_key,
                   format=self.format,
                   ignore=self.ignore_header)

        self.log.info("running sql statement: " + sql)
        redshift.run(sql)
        self.log.info('stagetoredshift operator completed completed')
