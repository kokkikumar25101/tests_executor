import os
import json
import sys
import asyncio
import aiobotocore
from line_utils_commons import logging
from aws_xray_sdk.core import xray_recorder, patch_all
from aws_xray_sdk.core.async_context import AsyncContext
from line_utils_commons.constants.lambda_constants import INVOCATION_TYPE_EVENT,INVOCATION_TYPE_REQUEST_RESPONSE
from line_utils_commons.exception import LineAwsClientError

logger = logging.getLogger(__name__)


is_lambda_environment = (os.getenv('IS_LAMBDA_EXECUTION_ENVIRONMENT') != None)


# xray_logger = logging.getLogger('aws_xray_sdk')
# xray_logger.setLevel(logger.get_logger_level())
# if is_lambda_environment:
#     patch_all()

# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logger.get_logger_level())
# logging.getLogger().addHandler(handler)

AWS_ACCESS_KEY_ID = os.environ['SERVICE_EXECUTION_USER_ACCESSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['SERVICE_EXECUTION_USER_SECRET']


def _xray_start(segment_name):
    if is_lambda_environment and xray_recorder:
        xray_recorder.begin_subsegment(segment_name)


def _xray_stop():
    if is_lambda_environment and xray_recorder:
        xray_recorder.end_subsegment()


def _xray_add_metadata(name, value):
    if is_lambda_environment and xray_recorder and xray_recorder.current_subsegment():
        return xray_recorder.current_subsegment().put_metadata(name, value)


async def _invoke(function_name, payload, session, invoke_semaphore, is_asynchronous):
    _xray_start('invoke_async')
    try:
        _xray_add_metadata('function_name', function_name)
        _xray_add_metadata('payload', payload)

        async with session.create_client(
                'lambda',
                region_name=os.environ['AWS_REGION_ID'],
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_access_key_id=AWS_ACCESS_KEY_ID
        ) as client:
            function_name = os.path.expandvars(
                f'arn:aws:lambda:$AWS_REGION_ID:$AWS_ACCOUNT_ID:function:$PARENT_STACKNAME-$ENVIRONMENT-{function_name}')
            # logger.debug(function_name)
            async with invoke_semaphore:
                resp = await client.invoke(
                    FunctionName=function_name,
                    Payload=json.dumps(payload),
                    InvocationType=INVOCATION_TYPE_EVENT if is_asynchronous else INVOCATION_TYPE_REQUEST_RESPONSE
                )
                payload = await resp['Payload'].read()
                return payload  # or assemble a dict with relevant parts
    except Exception as e:
        logger.error(f'Error in Async Ivoke {e.__class__})')
        raise LineAwsClientError(e) from e
    finally:
        _xray_stop()


def _generate_invocations(function_name, payloads, session, max_concurrent_tasks, is_asynchronous):
    invoke_semaphore = asyncio.BoundedSemaphore(max_concurrent_tasks)
    for payload in payloads:
        yield _invoke(function_name, payload, session, invoke_semaphore, is_asynchronous)


def invoke_all(function_name, payloads, max_concurrent_tasks=200, is_asynchronous=False, function_sequence_id=None):
    # loop = asyncio.get_event_loop()
    _xray_start('invoke_async')
    function_sequence_id = function_sequence_id if function_sequence_id else function_name
    try:
        _xray_add_metadata('function_name', function_name)
        _xray_add_metadata('payloads', payloads)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        xray_recorder.configure(service=f'{function_sequence_id}:{function_name}', context=AsyncContext(loop), sampling=False)
        # if check_in_lambda():
            # xray_recorder.configure(service=__name__, sampling=False, context=Context())
            # XRayMiddleware(app, xray_recorder)
            # patch_all()

        async def wrapped():
            session = aiobotocore.get_session()
            invocations = _generate_invocations(function_name, payloads, session, max_concurrent_tasks, is_asynchronous)
            return await asyncio.gather(*invocations)

        return loop.run_until_complete(wrapped())
    except Exception as e:
        logger.error(f'Error in Async Ivoke {e.__class__})')
        raise LineAwsClientError(e) from e
    finally:
        _xray_stop()


if __name__ == '__main__':
    data_key = 'STRIKEPRICES'
    symbol = ''
    option_type = 'CE'
    payloads_list = [{
        "input_config": {
            "identifier": {
                "option_type": option_type,
                "symbol": symbol
            },
            "data_key": data_key,
            "url": "http://nimblerest.lisuns.com:4531/GetStrikePrices/?accessKey=5582648f-b987-4318-aec9-0370acde5e05&exchange=NFO&instrumentType=OPTIDX&product=BANKNIFTY&expiry=0&optionType=CE"
        }
    }]

    lambda_responses = invoke_all('OhlcDataGDFRestClientFunction', payloads_list, is_asynchronous=False)

    logger.debug(lambda_responses)
