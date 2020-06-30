import time
import logging
import json

class Consume:
    def __init__(self, formatter, writer, filter_operations):
        self.cum_msg_count = 0
        self.cum_msg_size = 0
        self.msg_window_size = 0
        self.msg_window_count = 0
        self.cur_window = 0

        self.formatter = formatter
        self.writer = writer
        self.filter_operations = filter_operations

    def should_send_to_kinesis(self, fmt_msg):
        return fmt_msg.change.operation in self.filter_operations

    def __call__(self, change):
        self.cum_msg_count += 1
        self.cum_msg_size += change.data_size

        self.msg_window_size += change.data_size
        self.msg_window_count += 1

        fmt_msgs = self.formatter(change.payload)
        xid_msg = 'xid: {:12}'
        win_msg = 'win_count:{:>10} win_size:{:>10}mb'
        cum_msg = 'cum_count:{:>10} cum_size:{:>10}mb'
        progress_msg = f'{xid_msg} {win_msg} {cum_msg}'

        for fmt_msg in fmt_msgs:
            if self.should_send_to_kinesis(fmt_msg):
                # MAIN LINE HERE -- WORKS WITH ANY KINESIS PRODUCER
                # ALSO, NOT FMT_MSG...NEEDS TO BE BINARY
                stream_msg = json.dumps(fmt_msg.change.change)
                self.writer.put(stream_msg)
                change.cursor.send_feedback(flush_lsn=change.data_start)
                logging.info('Flushed LSN: {}'.format(change.data_start))

            int_time = int(time.time())
            if not int_time % 10 and int_time != self.cur_window:
                logging.info(progress_msg.format(
                    self.formatter.cur_xact, self.msg_window_count,
                    self.msg_window_size / 1048576, self.cum_msg_count,
                    self.cum_msg_size / 1048576))

                self.cur_window = int_time
                self.msg_window_size = 0
                self.msg_window_count = 0

