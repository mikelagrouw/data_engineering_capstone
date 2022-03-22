from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow import settings
from airflow.models import Connection

class CreateConnections(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 conn_type  = '',
                 host  = '',
                 login  = '',
                 pwd  = '',
                 port  = 0,
                 schema = '', *args, **kwargs):
        super(CreateConnections, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.pwd = pwd
        self.port = port
        self.schema = schema

    def execute(self, **kwargs):
        conn = Connection(conn_id=self.conn_id,
                          conn_type=self.conn_type,
                          host=self.host,
                          login=self.login,
                          password=self.pwd,
                          port=self.port,
                          schema = self.schema
                          )
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if str(conn_name) == str(conn.conn_id):
            logging.warning(f"Connection {conn.conn_id} already exists, remove before adding again with changes")
        else:
            session.add(conn)
            session.commit()
            logging.info(Connection.log_info(conn))
            logging.info(f'Connection {self.conn_id} is created')
