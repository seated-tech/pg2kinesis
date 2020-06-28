from __future__ import division
import time

import click

from .slot import SlotReader
from .formatter import get_formatter
from .stream import StreamWriter
from .consumer import Consume
from .log import logger


SUPPORTED_OPERATIONS = ['update', 'insert', 'delete', 'truncate']


@click.command()
@click.option('--pg-dbname', '-d', help='Database to connect to.')
@click.option('--pg-host', '-h', default='',
              help='Postgres server location. Leave empty if localhost.')
@click.option('--pg-port', '-p', default='5432', help='Postgres port.')
@click.option('--pg-user', '-u', help='Postgres user')
@click.option('--pg-sslmode', help='Postgres SSL mode', default='prefer')
@click.option('--pg-slot-name', '-s', default='pg2kinesis',
              help='Postgres replication slot name.')
@click.option('--pg-slot-output-plugin', default='test_decoding',
              type=click.Choice(['test_decoding', 'wal2json']),
              help='Postgres replication slot output plugin')
@click.option('--stream-name', '-k', default='pg2kinesis',
              help='Kinesis stream name.')
@click.option('--message-formatter', '-f', default='CSVPayload',
              type=click.Choice(['CSVPayload', 'CSV']),
              help='Kinesis record formatter.')
@click.option('--table-pat', help='Optional regular expression for table names.')
@click.option('--full-change', default=False, is_flag=True,
              help='Emit all columns of a changed row.')
@click.option('--create-slot', default=False, is_flag=True,
              help='Attempt to on start create a the slot.')
@click.option('--recreate-slot', default=False, is_flag=True,
              help='Deletes the slot on start if it exists and then creates.')
@click.option('--operations', default='all', type=click.Choice(['all'] + SUPPORTED_OPERATIONS),
              multiple=True, help = 'Which operations to replicate to kinesis, Default: all')
def main(pg_dbname, pg_host, pg_port, pg_user, pg_sslmode, pg_slot_name, pg_slot_output_plugin,
         stream_name, message_formatter, table_pat, operations, full_change, create_slot, recreate_slot):
    if 'all' in operations:
        operations = SUPPORTED_OPERATIONS

    if full_change:
        assert message_formatter == 'CSVPayload', 'Full changes must be formatted as JSON.'
        assert pg_slot_output_plugin == 'wal2json', 'Full changes must use wal2json.'

    logger.info('Starting pg2kinesis replicating the following operations: %s', ','.join(operations))
    logger.info('Getting kinesis stream writer')
    writer = StreamWriter(stream_name)

    with SlotReader(pg_dbname, pg_host, pg_port, pg_user, pg_sslmode, pg_slot_name,
                    pg_slot_output_plugin) as reader:

        if recreate_slot:
            reader.delete_slot()
            reader.create_slot()
        elif create_slot:
            reader.create_slot()

        pk_map = reader.primary_key_map
        formatter = get_formatter(message_formatter, pk_map,
                                  pg_slot_output_plugin, full_change, table_pat)

        consume = Consume(formatter, writer, operations)

        # Blocking. Responds to Control-C.
        reader.process_replication_stream(consume)

if __name__ == '__main__':
    main()
