import json
import os

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration, SQSConfiguration, StateMachineConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library.interfaces.sqs_interface import SQSInterface
from datalake_library.interfaces.states_interface import StatesInterface

logger = init_logger(__name__)


def fetch_messages(team, pipeline, stage):
    dynamo_config = DynamoConfiguration()
    dynamo_interface = DynamoInterface(dynamo_config)
    pipeline_info = dynamo_interface.get_pipelines_table_item(f"{team}-{pipeline}-{stage}")
    min_items_to_process = 1
    max_items_to_process = 100
    logger.info(f"Pipeline is {pipeline}, stage is {stage}")
    logger.info(f"Details from DynamoDB: {pipeline_info.get('pipeline', {})}")
    min_items_to_process = pipeline_info["pipeline"].get("min_items_process", min_items_to_process)
    max_items_to_process = pipeline_info["pipeline"].get("max_items_process", max_items_to_process)

    messages = []

    sqs_config = SQSConfiguration(team, pipeline, stage)
    queue_interface = SQSInterface(sqs_config.get_stage_queue_name)
    logger.info(f"Querying {team}-{pipeline}-{stage} objects waiting for processing")
    messages = queue_interface.receive_min_max_messages(min_items_to_process, max_items_to_process)

    logger.info(f"{len(messages)} Objects ready for processing")
    messages = list(set(messages))

    return messages


def lambda_handler(event, context):
    try:
        keys_to_process = []
        trigger_type = event.get("trigger_type")  # this is set by the schedule event rule, so if there is any value, this is a scheduled run
        if trigger_type:
            records = fetch_messages(event["team"], event["pipeline"], event["pipeline_stage"])
        else:
            records = event["Records"]
        logger.info(f"Received {len(records)} messages")

        pipeline = os.environ["PIPELINE"]
        pipeline_stage = os.environ["PIPELINE_STAGE"]
        org = os.environ["ORG"]
        domain = os.environ["DOMAIN"]
        env = os.environ["ENV"]
        # the team should be known as well given a pipeline is deployed by a team. no need to get it from whatever is coming
        # let's imagine use cases
        # #1: push to s3 raw following the sdlf convention team/dataset/whatever/maybe.json
        # #2: push to s3 raw NOT following the sdlf convention blobliblob/whatevermaybe.json
        # #3: not a push to s3 but I want to execute a lambda based on an event trigger

configure datasetname when deploying pipeline - lose ability to use pipeline for multiple datasets
if dataset is provided in source event somehow you can still have that
why do i want to change that exactly - always a good question

the answer to this last question is:
I want an agnostic "run lambda transform" stage.
The run lambda transform should:
  run a specified transform lambda
  handle service issues with a retry-backoff strategy
  provide tracing through xray
  handle error, send a notification, redrive and partial redrive
  be idempotent?

and what's the point. what's the use case.
  

if you don't have sdlf-team/foundations:
  hoho
  no notification unless you create your sns topic
  you need to create your own sns topic

        for record in records:
            logger.info("Starting State Machine Execution")
            event_body = json.loads(record["body"])
            object_key = event_body["detail"]["object"]["key"].split("/")
            team = object_key[0]
            dataset = object_key[1]

            event_with_pipeline_details = {
                **event_body["detail"]["object"],
                "bucket": event_body["detail"]["bucket"]["name"],
                "team": team,
                "dataset": dataset,
                "pipeline": pipeline,
                "pipeline_stage": pipeline_stage,
                "org": org,
                "domain": domain,
                "env": env,
            }

            state_config = StateMachineConfiguration(team, pipeline, pipeline_stage)
            StatesInterface().run_state_machine(
                state_config.get_stage_state_machine_arn, json.dumps(event_with_pipeline_details)
            )
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
