from collections import namedtuple
import logging
import threading

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import psycopg2.errorcodes

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, None)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, None)

PrimaryKeyMapItem = namedtuple('PrimaryKeyMapItem', 'table_name, col_name, col_type, col_ord_pos')


class SlotReader(object):
    PK_SQL = """
    SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, ordinal_position
    FROM information_schema.tables
    LEFT JOIN (
        SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, c.ordinal_position,
                    table_catalog, table_schema, table_name
        FROM information_schema.table_constraints
        JOIN information_schema.key_column_usage AS kcu
            USING (constraint_catalog, constraint_schema, constraint_name,
                        table_catalog, table_schema, table_name)
        JOIN information_schema.columns AS c
            USING (table_catalog, table_schema, table_name, column_name)
        WHERE constraint_type = 'PRIMARY KEY'
    ) as q using (table_catalog, table_schema, table_name)
    ORDER BY ordinal_position;
    """

    def __init__(self, database, host, port, user, pwd, sslmode, slot_name, tables,
                 schema="public", output_plugin='wal2json'):
        # Cool fact: using connections as context manager doesn't close them on
        # success after leaving with block
        self._db_confg = dict(database=database, host=host, port=port, user=user, password=pwd, sslmode=sslmode)
        self._repl_conn = None
        self._repl_cursor = None
        self._normal_conn = None
        self.slot_name = slot_name
        self.schema = schema
        self.tables = tables
        self.output_plugin = output_plugin
        self.cur_lag = 0

    def __enter__(self):
        self._normal_conn = self._get_connection()
        self._normal_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._repl_conn = self._get_connection(connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self._repl_cursor = self._repl_conn.cursor()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Be a good citizen and try to clean up on the way out.
        """
        try:
            self._repl_cursor.close()
        except Exception:
            pass

        try:
            self._repl_conn.close()
        except Exception:
            pass

        try:
            self._normal_conn.close()
        except Exception:
            pass

    def _format_tables(self):
        return ",".join([f"{self.schema}.{tbl}" for tbl in self.tables])


    def _get_connection(self, connection_factory=None, cursor_factory=None):
        return psycopg2.connect(connection_factory=connection_factory,
                                cursor_factory=cursor_factory, **self._db_confg)

    def _execute_and_fetch(self, sql, *params):
        with self._normal_conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            return cur.fetchall()

    @property
    def primary_key_map(self):
        logging.info('Getting primary key map')
        result = map(PrimaryKeyMapItem._make, self._execute_and_fetch(SlotReader.PK_SQL))
        pk_map = {rec.table_name: rec for rec in result}

        return pk_map

    def create_slot(self):
        logging.info('Creating slot %s' % self.slot_name)
        try:
            self._repl_cursor.create_replication_slot(self.slot_name,
                                                      slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                      output_plugin=self.output_plugin)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                logging.error(p)
                raise
            else:
                logging.info('Slot %s is already present.' % self.slot_name)

    def delete_slot(self):
        logging.info('Deleting slot %s' % self.slot_name)
        try:
            self._repl_cursor.drop_replication_slot(self.slot_name)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.UNDEFINED_OBJECT:
                logging.error(p)
                raise
            else:
                logging.info('Slot %s was not found.' % self.slot_name)

    def process_replication_stream(self, consume):
        if self.output_plugin == 'wal2json':
            options = {
                'include-xids': 1,
                'include-timestamp': 1,
                'include-types': False,
                'write-in-chunks': True
            }
            if self.tables is not None:
                if self.tables != "all":
                    options['add-tables'] = self._format_tables()
        else:
            options = None
        logging.info(f'Starting the consumption of slot {self.slot_name}, {options}')
        self._repl_cursor.start_replication(self.slot_name, options=options)
        self._repl_cursor.consume_stream(consume)
