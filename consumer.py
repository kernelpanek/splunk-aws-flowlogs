#!/usr/bin/env python
from time import sleep, time, strftime, localtime, mktime
from random import randint
from itertools import chain
from datetime import datetime, timedelta
import argparse
import multiprocessing as mp
import boto3
import logging
import pickle
import sys
import os
import pwd

def getAllLogGroups(conn):
    return conn.describe_log_groups()['logGroups']

def getAllLogStreams(conn):
    all_streams = []
    for grp in getAllLogGroups(conn):
        grp_streams = conn.describe_log_streams(logGroupName=grp.get('logGroupName'))
        all_streams.append([(grp.get('logGroupName'), stream) for stream in grp_streams.get('logStreams')])
        next_token = None
        while (grp_streams.get('nextToken') is not None and grp_streams.get('nextToken') != next_token):
            next_token = grp_streams.get('nextToken')
            try:
                grp_streams = conn.describe_log_streams(logGroupName=grp.get('logGroupName'), nextToken=next_token)
            except Exception as ex:
                logger.error(ex)
                sleep(randint(100,500) / 1000)
                grp_streams = conn.describe_log_streams(logGroupName=grp.get('logGroupName'), nextToken=next_token)

            all_streams.append([(grp.get('logGroupName'), stream) for stream in grp_streams.get('logStreams')])
    return list(chain(all_streams))

def getAllEvents(workload, queue, conn):
    log_group_name = workload.get('log_group_name')
    log_stream_name = workload.get('log_stream_name')
    start_time = workload.get('start_time')
    next_token = workload.get('next_token')

    raw_events = []
    back_token_tracker = None
    sleep(randint(100,500) / 1000)
    logger.debug("log_group_name: {0}; log_stream_name: {1}; start_time: {2}; next_token: {3};".format(log_group_name, log_stream_name, start_time, next_token))
    if start_time is None:
        if next_token is None:
            event_cursor = conn.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name)
        else:
            event_cursor = conn.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, nextToken=next_token)
    else:
        if next_token is None:
            event_cursor = conn.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, startTime=start_time)
        else:
            event_cursor = conn.get_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, startTime=start_time, nextToken=next_token)

    raw_events.append([workload, event_cursor.get('events')])
    back_token_tracker = event_cursor.get('nextBackwardToken')
    if back_token_tracker and back_token_tracker != next_token:
        workload['next_token'] = back_token_tracker
        queue.put(workload)
    return raw_events

def processData(completed_work_queue, pending_work_queue, interval, output_file):
    try:
        if output_file is not None: sys.stdout = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), output_file), 'a')
        while completed_work_queue.empty():
            sleep(1)
        while not completed_work_queue.empty() or not pending_work_queue.empty():
            logger.info("completed_work_queue: {0};".format(completed_work_queue.qsize()))
            logger.info("pending_work_queue: {0};".format(pending_work_queue.qsize()))
            flushQueue(completed_work_queue)
            sleep(interval)
    except Exception as ex:
        logger.error(ex)
    finally:
        logger.info("data writer terminating.")
        sys.exit(0)

def workOnQueue(pending_queue, complete_queue, conn):
    isFinished = False
    while not pending_queue.empty() or not isFinished:
        try:
            obj = pending_queue.get(timeout=10)
            all_events = getAllEvents(obj, pending_queue, conn)
            complete_queue.put(all_events)
        except:
            if 'queue.empty' in str(sys.exc_info()[0]).lower():
                pass
            else:
                pending_queue.put(obj)
                logger.error("workOnQueue-failure: Attempting to put work back on the queue")
                logger.error(sys.exc_info()[0])
        finally:
            isFinished = complete_queue.empty()
            logger.info("pending_queue.empty = {0}. isFinished = {1}.".format(pending_queue.empty(), isFinished))
    logger.info("worker process exiting.")
    sys.exit(0)

def flushQueue(queue):
    while not queue.empty():
        log = queue.get(timeout=10)
        for log_segment in log:
            log_group = log_segment[0]['log_group_name']
            [sys.stdout.write("{0} {1} {2}\n".format(event['timestamp'], event['message'], log_group)) for event in log_segment[1]]
    logger.info('completed work flushed')

def parse_timedelta(timespan):
    t = datetime.strptime(timespan,"%H:%M:%S")
    delta = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
    return delta

def make_aws_start_timestamp(timespan):
    now = time()
    dt = datetime.fromtimestamp(now) - parse_timedelta(timespan)
    return int(mktime(dt.timetuple()) * 1000)

class ProcManager:
    def __init__(self, args):
        self.args = args
        self.mgr = mp.Manager()
        self.pending_work_queue = self.mgr.Queue()
        self.completed_work_queue = self.mgr.Queue()
        self.mgr_attrs = self.mgr.dict()
        self.mgr_attrs['time_marker'] = make_aws_start_timestamp(self.args.timespan)
        logger.info("Retrieving logs starting at {0}".format(strftime('%Y-%m-%d %H:%M:%S', localtime(self.mgr_attrs['time_marker']/1000))))
        self.aws = boto3.session.Session(region_name='us-east-1') #todo: CLIze/Config
        self.logconn = self.aws.client('logs')


    def initializePendingQueueWorkload(self, queue, conn):
        for streams in getAllLogStreams(conn):
            for stream in streams:
                startTimestamp = 0
                if self.mgr_attrs['time_marker'] == 0:
                    startTimestamp = stream[1].get('firstEventTimestamp')
                else:
                    startTimestamp = self.mgr_attrs['time_marker']

                workload = {'log_group_name': stream[0],
                            'log_stream_name': stream[1].get('logStreamName'),
                            'start_time': startTimestamp,
                            'next_token': None}
                queue.put(workload)
                logger.info("workload added: {0}".format(workload))
        logger.info("pending_work_queue initialized")
        logger.info("pending_work_queue: {0};".format(queue.qsize()))

    def start(self):
        self.initializePendingQueueWorkload(self.pending_work_queue, self.logconn)
        self.workers = [mp.Process(target=workOnQueue, args=(self.pending_work_queue, self.completed_work_queue, self.logconn,))
                        for i in range(self.args.worker_count)]
        for w in self.workers:
            w.start()
            logger.info("worker initialized: PID: {0}".format(w.pid))


        self.saver = mp.Process(target=processData, args=(self.completed_work_queue, self.pending_work_queue, self.args.write_interval, self.args.output_file,))
        self.saver.start()
        logger.info("data writer initialized: PID: {0}".format(self.saver.pid))

    def join(self):
        self.saver.join()
        for w in self.workers:
            w.join()

    def terminate(self):
        for w in self.workers:
            logger.info("worker terminating: PID: {0}".format(w.pid))
            w.terminate()
        logger.info("data writer terminating: PID: {0}".format(self.saver.pid))
        self.saver.terminate()
        logger.info("workers terminated")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AWS FlowLogs Retrieval Processor')
    parser.add_argument('-t', '--timespan', dest='timespan', type=str, default='00:10:00',
                        help='timespan to consume in number of hours:minutes:seconds from now')
    parser.add_argument('-w', '--workers', dest='worker_count', type=int, default=6,
                       help='number of concurrent worker processes to retrieve data')
    parser.add_argument('-i', '--interval', dest='write_interval', type=int, default=30,
                       help='amount of time to wait between dumping the completed work to file')
    parser.add_argument('-l', '--logfile', dest='log_file', type=str, default='awsflowlogs.log',
                       help='log file name')
    parser.add_argument('-f', '--file', dest='output_file', type=str, default=None,
                       help='output file name')
    args = parser.parse_args()

    logging.getLogger('').handlers = []
    logging.basicConfig(filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), args.log_file),
                        level=logging.ERROR,
                        format="%(asctime)s %(process)d %(funcName)s:%(lineno)d %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    aws = boto3.session.Session(region_name='us-east-1')
    logconn = aws.client('logs')

    logger.info("consumer started. PID: {0}".format(os.getpid()))
    pm = ProcManager(args)
    pm.start()
    pm.join()
    pm.terminate()
    logger.info('consumer finished')
    sys.exit(0)
