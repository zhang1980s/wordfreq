import boto3
import time
import os

def get_sqs_queue_length(qurl):

    sqs = boto3.client('sqs')
    response = sqs.get_queue_attributes(
        QueueUrl = qurl,
        AttributeNames = ['ApproximateNumberOfMessages'])

    return {
        'queue_length': response['Attributes']['ApproximateNumberOfMessages']
    }

def get_inservice_count(asgname):

    asg = boto3.client('autoscaling')
    response = asg.describe_auto_scaling_groups(
        AutoScalingGroupNames = [asgname]
    )

    if len(response['AutoScalingGroups']) == 1:
        InServiceCount = len(list(filter(lambda x: 'LifecycleState' in x and x ['LifecycleState'] == 'InService', response['AutoScalingGroups'][0]['Instances'])))
    
    return {
        'InServiceCount': InServiceCount
    }

def put_backlog_data(backlog_per_instance):
    cw = boto3.client('cloudwatch')

    response = cw.put_metric_data(
        Namespace = 'Wordfreq',
        MetricData = [
            {
                'MetricName': 'WordfreqBacklogPerInstance',
                'Dimensions': [
                    {
                        'Name': 'Wordfreq',
                        'Value': 'Wordfreq'
                    },
                ],
                'Value': float(backlog_per_instance),
                'Unit': 'None'
            }
        ]
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200 :
        return {
            'put_metric_message': 'Upload BacklogPerInstance '+str(backlog_per_instance)+' to cloudwatch at '+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }



def lambda_handler(event, context):

    # qurl = 'https://sqs.us-east-1.amazonaws.com/894855526703/wordfreq-jobs'
    qurl = os.environ['SQS_WORDFREQ_JOBS_URL']
    queue_length = get_sqs_queue_length(qurl)
    print('SQS_queue_size:',queue_length['queue_length'])

    #asgname = 'asg-wordfreq-app'
    asgname = os.environ['WORDFREQ_ASG_NAME']
    InServiceCount = get_inservice_count(asgname)

    print('InServiceCount:',InServiceCount['InServiceCount'])

    backlog_per_instance = int(queue_length['queue_length']) / InServiceCount['InServiceCount']

    print('backlog per instance:', backlog_per_instance)

    response = put_backlog_data(backlog_per_instance)

    print('put_metric_message:',response['put_metric_message'])
