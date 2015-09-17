#!/usr/bin/env python
import boto3
import botocore.exceptions
import configparser
import io, os, pwd, sys, re
import logging
from time import sleep, time, strftime, localtime, mktime
from random import randint
from itertools import chain
from datetime import datetime, timedelta
import argparse
import multiprocessing as mp
import pickle

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
        strikes = 0
        if output_file is not None: sys.stdout = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), output_file), 'a')
        while completed_work_queue.empty():
            if strikes > 10: break
            sleep(1)
            if pending_work_queue.empty(): strikes += 1

        while not completed_work_queue.empty() or not pending_work_queue.empty():
            logger.info("completed_work_queue: {0};".format(completed_work_queue.qsize()))
            logger.info("pending_work_queue: {0};".format(pending_work_queue.qsize()))
            flushQueue(completed_work_queue)
            sleep(interval)
    except Exception as ex:
        logger.error(ex)
    logger.info("data writer terminating.")

def workOnQueue(pending_queue, complete_queue, conn):
    isFinished = False
    while not pending_queue.empty() or not isFinished:
        try:
            obj = pending_queue.get(timeout=20)
            all_events = getAllEvents(obj, pending_queue, conn)
            complete_queue.put(all_events)
            pending_queue.task_done()
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

def flushQueue(queue):
    while not queue.empty():
        log = queue.get(timeout=20)
        for log_segment in log:
            log_group = log_segment[0]['log_group_name']
            [sys.stdout.write("{0} {1} {2}\n".format(event['timestamp'], event['message'], log_group)) for event in log_segment[1]]
        queue.task_done()
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
    def __init__(self, args, aws_profile='default'):
        self.args = args
        self.mgr = mp.Manager()
        self.pending_work_queue = self.mgr.Queue()
        self.completed_work_queue = self.mgr.Queue()
        self.mgr_attrs = self.mgr.dict()
        self.mgr_attrs['time_marker'] = make_aws_start_timestamp(self.args.timespan)
        logger.info("Retrieving logs starting at {0}".format(strftime('%Y-%m-%d %H:%M:%S', localtime(self.mgr_attrs['time_marker']/1000))))
        self.aws = boto3.session.Session(profile_name=aws_profile)
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

        sleep(3)
        self.saver = mp.Process(target=processData, args=(self.completed_work_queue, self.pending_work_queue, self.args.write_interval, self.args.output_file,))
        self.saver.start()
        logger.info("data writer initialized: PID: {0}".format(self.saver.pid))

    def join(self):
        for w in self.workers:
            w.join()
        self.saver.join()

    def terminate(self):
        for w in self.workers:
            logger.info("worker terminating: PID: {0}".format(w.pid))
            w.terminate()
        logger.info("data writer terminating: PID: {0}".format(self.saver.pid))
        self.saver.terminate()
        logger.info("workers terminated")

class FlowlogSetup():

    def __init__(self, profile_name='default', region='us-east-1'):
        self.aws_session = boto3.session.Session(profile_name=profile_name, region_name=region)
        self.iam = self.aws_session.client('iam')
        self.ec2 = self.aws_session.client('ec2')

    def find_missing_flowlogged_vpcs(self):
        vpc_ids = [vpc['VpcId'] for vpc in self.ec2.describe_vpcs().get('Vpcs')]
        log_resource_ids = [lg['ResourceId'] for lg in self.ec2.describe_flow_logs().get('FlowLogs')]
        return set(vpc_ids).difference(log_resource_ids)

    def find_flowlogs_iam_role(self):
        necessary_assume_action_set = set(['sts:AssumeRole'])
        necessary_actions_set = set(['logs:CreateLogGroup',
                                     'logs:CreateLogStream',
                                     'logs:DescribeLogGroups',
                                     'logs:DescribeLogStreams',
                                     'logs:PutLogEvents'])

        roles_found = []
        roles = self.iam.list_roles().get('Roles')
        for r in roles:
            assumption_found = False
            actions_found = False
            assume_role_policy_document = r.get('AssumeRolePolicyDocument')
            statements = assume_role_policy_document.get('Statement')
            for statement in statements:
                if (necessary_assume_action_set.issubset(set([statement.get('Action')])) and
                    statement.get('Effect') == 'Allow' and
                    statement.get('Principal') == {'Service': 'vpc-flow-logs.amazonaws.com'}):
                        assumption_found = True
            policy_names = self.iam.list_role_policies(RoleName=r.get('RoleName')).get('PolicyNames')
            for policy in policy_names:
                policy_document = self.iam.get_role_policy(RoleName=r.get('RoleName'), PolicyName=policy).get('PolicyDocument')
                statements = policy_document.get('Statement')
                for statement in statements:
                    if statement.get('Effect') == 'Allow':
                        actions = statement.get('Action')
                        if (actions is not None and necessary_actions_set.issubset(set(actions))):
                            actions_found = True
            if (assumption_found and actions_found):
                roles_found.append(r)
        return(roles_found)

    def create_flowlogs_iam_role(self, role_name):
        assume_role_policy_document = '{"Statement": [{"Action": "sts:AssumeRole","Effect": "Allow","Principal": {"Service": "vpc-flow-logs.amazonaws.com"},"Sid": ""}],"Version": "2012-10-17"}'
        policy_name='auto_flowlogsRole'
        role_policy_document = '{"Statement": [{"Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:DescribeLogGroups","logs:DescribeLogStreams","logs:PutLogEvents"],"Effect": "Allow","Resource": "*"}]}'
        create_role_result = self.iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document)
        put_policy_result = self.iam.put_role_policy(RoleName=role_name, PolicyName=policy_name, PolicyDocument=role_policy_document)
        return(create_role_result, put_policy_result)

    def create_flowlogs(self, vpc_id, log_delivery_arn):
        self.ec2.create_flow_logs(TrafficType='ALL', ResourceIds=[vpc_id], ResourceType='VPC', DeliverLogsPermissionArn=log_delivery_arn, LogGroupName="{0}-all".format(vpc_id))

    def create_flowlogs_iam_user(self, user_name):
        try:
            self.iam.create_user(UserName=user_name)
            self.iam.attach_user_policy(UserName=user_name, PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsReadOnlyAccess')
            return(self.iam.create_access_key(UserName=user_name))
        except botocore.exceptions.ClientError as CE:
            if CE.response.get('Error').get('Code') == 'EntityAlreadyExists':
                print('User already exists.')

    def rotate_flowlogs_iam_user_keys(self, user_name):
        for key in self.iam.list_access_keys(UserName=user_name).get('AccessKeyMetadata'):
            self.iam.delete_access_key(UserName=user_name, AccessKeyId=key.get('AccessKeyId'))
        return(self.iam.create_access_key(UserName=user_name))

    def ensure_flowlogs_iam_role(self):
        arn = ''
        log_roles = self.find_flowlogs_iam_role()
        if log_roles is None or len(log_roles) < 1:
            result = self.create_flowlogs_iam_role('flowlogsRole')
            arn = result[0].get('Arn')
        else:
            arn = log_roles[0].get('Arn')
        return(arn)

    def ensure_flowlogs_iam_user(self, user_name):
        try:
            return self.rotate_flowlogs_iam_user_keys(user_name)
        except botocore.exceptions.ClientError as CE:
            if CE.response.get('Error').get('Code') == 'NoSuchEntity':
                return self.create_flowlogs_iam_user(user_name)

    def ensure_vpc_flowlog_groups(self):
        arn = self.ensure_flowlogs_iam_role()
        print(arn)
        vpc_ids = self.find_missing_flowlogged_vpcs()
        for vpc_id in vpc_ids:
            self.create_flowlogs(vpc_id, arn)

class FlowlogConfigUtils:

    @staticmethod
    def profile_names(conf_file_path, exclude = []):
        config = configparser.ConfigParser()
        config.read(conf_file_path)
        profile_names = config.sections()
        profile_list = set(profile_names).difference(set(exclude))
        return profile_list

    @staticmethod
    def write_flowlog_consumer_config_file(conf_file_path, iam_user_name):
        accounts = {}
        for profile in FlowlogConfigUtils.profile_names():
            flog = FlowlogSetup(profile)
            accounts[profile] = flog.ensure_flowlogs_iam_user(iam_user_name)

        with open(conf_file_path, 'w+') as conf_file:
            awsconf = configparser.ConfigParser()
            awsconf.read_file(conf_file)
            for acct in accounts:
                if not awsconf.has_section(acct): awsconf.add_section(acct)
                awsconf.set(acct, 'output', 'json')
                awsconf.set(acct, 'region', 'us-east-1')
                awsconf.set(acct, 'aws_access_key_id', accounts[acct]['AccessKey']['AccessKeyId'])
                awsconf.set(acct, 'aws_secret_access_key', accounts[acct]['AccessKey']['SecretAccessKey'])
            awsconf.write(conf_file)

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
    parser.add_argument('-c', '--config', dest='aws_config_file', type=str, default='/root/.aws/config',
                       help='An Amazon Web Servies configuration file')
    args = parser.parse_args()

    logging.getLogger('').handlers = []
    logging.basicConfig(filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), args.log_file),
                        level=logging.ERROR,
                        format="%(asctime)s %(process)d %(funcName)s:%(lineno)d %(name)s %(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)


    logger.info("consumer started. PID: {0}".format(os.getpid()))
    aws_profiles = FlowlogConfigUtils.profile_names(args.aws_config_file)
    proc_managers = {}
    for profile in aws_profiles:
        proc_managers[profile] = ProcManager(args, profile)
        proc_managers[profile].start()
        proc_managers[profile].join()
        proc_managers[profile].terminate()
    logger.info('consumer finished')
    sys.exit(0)
