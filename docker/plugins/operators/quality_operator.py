"""Run quality checks."""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Run quality checks."""

    ui_color = '#339966'

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 tests=[],
                 *args, **kwargs):
        """Initialize."""
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.tests = tests

    def execute(self, context):
        """Execute quality checks."""
        for test in self.tests:
            self.log.info(f"This is the query: {list(test.keys())[0]}")
            redshift_hook = PostgresHook(self.conn_id)

            records = redshift_hook.get_records(list(test.keys())[0])

            num_records = len(records)

            # if not empty is given as expected value check if table is empty
            if list(test.values())[0] == 'not empty':
                if num_records < 1:
                    raise ValueError(f"""Data quality check failed.
                                     {list(test.keys())[0]} returned 0 rows""")
            # else check if num records is expected value
            else:
                if num_records != list(test.values())[0]:
                    raise ValueError(f"""Data quality check failed.
                                     {list(test.keys())[0]} returned
                                     {num_records} rows, while
                                     {list(test.values())[0]} were expected""")

            self.log.info(f"""Data quality on query (
                          {list(test.keys())[0]})
                          check passed with {num_records} records""")
