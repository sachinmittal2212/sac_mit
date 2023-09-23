import json
from croniter import croniter
from datetime import datetime
import os
from pathlib import Path
from helper.aws_conn import SecretManagerConn
from helper.config_reader import XMLConfig
from helper.aws_conn import S3Conn, SQSConn
from helper.handler import FileManipulation
from helper.logging import logger, LambdaContext


def get_cfg_dict_from_xml(config, validate, S3=None):
    from os import environ
    if environ.get("AWS_EXECUTION_ENV") is not None:
        config_xml = S3.get_raw_data_from_S3(config)
        validate = S3.get_raw_data_from_S3(validate)
        cfg = XMLConfig(xml_data=config_xml, xsd_data=validate)
        logger.info("Reading config and validation is completed.")
        cfg_dict = cfg.xml_to_dict()
    else:
        local_cfg_loc = Path('../test/')/config
        local_xsd_loc = Path('../test/')/validate
        cfg_file = open(local_cfg_loc, 'rb').read()
        validate = open(local_xsd_loc, 'rb').read()
        cfg = XMLConfig(xml_data=cfg_file, xsd_data=validate,
                            xml_path=local_cfg_loc, xsd_path=local_xsd_loc)
        cfg_dict = cfg.xml_to_dict()
    return cfg_dict


def updating_process_dict(prc_dict):
    next_run_list = []
    now = datetime.utcnow()
    dt_format = '%Y-%m-%d %H:%M:%S'
    prc_next_run = datetime.strptime(prc_dict['NextRun'], dt_format)
    if(prc_next_run < now):
        iter = croniter(prc_dict['Cron'], now)
        next_run_dt = iter.get_next(datetime)
        next_run_list.append([prc_dict['Name'], prc_dict['OtherParam'],
                              prc_dict['NextRun']])
        prc_dict['NextRun'] = next_run_dt
    return (next_run_list, prc_dict)


def find_next_runs(processes_list):
    if processes_list is list:
        for process_dict in processes_list:
            (next_run_list, process_dict) = updating_process_dict(process_dict)
    else:
        logger.info('Only process found in the schedule file.')
        (next_run_list, processes_list) = updating_process_dict(processes_list)
    return (next_run_list, processes_list)


# def send_to_queue(MsgBody):
#     sqs = boto3.resource('sqs')
#     queue = sqs.get_queue_by_name(QueueName='ivz-dev-0065-crd-batch.fifo')
#     # response = queue.send_message(MessageBody='Hello')testmessage
#     # response = queue.send_message(MessageBody='{{"job":"a_job","data":{"some":"variable"}}', MessageGroupId='586474de88e03')
#     try:
#         response = queue.send_message(
#             MessageBody=MsgBody, MessageGroupId='CRD_ESG')
#     except:
#         print('Unable to send the data to SQS Queue')
#         raise
#     return response


def updating_sch_in_S3(sch_dict, S3_conn, S3Key=None):
    FM = FileManipulation()
    xml_str = FM.get_xml_from_dict(sch_dict, "    ")
    f_dt = {'name': 'schedule', 'ext':'xml'}
    local_key = FM.get_file_path(f_dict=f_dt, local=True)
    FM.put_str_to_local(str=xml_str, local_key=local_key)
    if os.environ.get("AWS_EXECUTION_ENV") is not None:
        logger.info('Copying file {} to S3 {}'.format(local_key, S3Key))
        S3_conn.put_file_to_s3(key=S3Key, file_loc=local_key)
    else:
        from shutil import copy
        int_path = Path('..', 'test', S3Key)
        logger.info('Copying file {} to S3 {}'.format(local_key, int_path))
        copy(local_key, int_path)

@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    logger.info('Starting with event: {}'.format(event)) 
    try:
        S3 = S3Conn('ivz-dev-0065-crd-batch-ue1')
        sqs = SQSConn(queue_name='ivz-dev-0065-crd-batch.fifo')
    except Exception as e:
        logger.exception('Failing in connecting to AWS Resources. \
            Error: {}'.format(e))
        raise


    sch_kwd = 'scheduler/schedule.xml'
    val_sch_key = 'crd_batch_interface/validator/schedule_validate.xsd'
    src_folder = 'crd_batch_interface'

    # get the list of scheduler files
    sch_list = S3.get_file_from_keyword(prefix=src_folder, keyword=sch_kwd)

    # Reading scheduler file
    for sch_key in sch_list:
        logger.info("Looking into {} schedule.".format(sch_key))
        sch_dict = get_cfg_dict_from_xml(sch_key, val_sch_key, S3)
        processes_list = sch_dict['Scheduler']['Process']
        (next_run_list, upd_prc_ls) = find_next_runs(processes_list)
        sch_dict['Scheduler']['Process'] = upd_prc_ls
        try:
          # Sending config details to SQS
          for config in next_run_list:
            sqs_msg={}
            sqs_msg[config[0]]=[config[1], str(config[2])]
            logger.info('Sending {} to SQS queue.'.format(sqs_msg))
            response=sqs.push_msg_to_queue(json.dumps(sqs_msg))
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
              logger.info('Issue in sending the message to SQS.')
              raise Exception('Unable to send response to SQS. {}'.format(response))
        except :
            raise
        #   raise Exception(e)
        updating_sch_in_S3(sch_dict=sch_dict, S3_conn=S3, S3Key=sch_key)
