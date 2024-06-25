import os
import json
import sys
import asyncio
import aiobotocore
from line_utils_commons import logging
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.lambda_launcher import check_in_lambda
# from aws_xray_sdk.ext.flask.middleware import XRayMiddleware
from aws_xray_sdk.core.async_context import AsyncContext
from line_utils_commons.constants.lambda_constants import INVOCATION_TYPE_EVENT,INVOCATION_TYPE_REQUEST_RESPONSE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

xray_logger = logging.getLogger('aws_xray_sdk')
xray_logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

AWS_ACCESS_KEY_ID = os.environ['SERVICE_EXECUTION_USER_ACCESSKEY']
AWS_SECRET_ACCESS_KEY = os.environ['SERVICE_EXECUTION_USER_SECRET']


async def _invoke(function_name, payload, session, invoke_semaphore, is_asynchronous):
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


def _generate_invocations(function_name, payloads, session, max_concurrent_tasks, is_asynchronous):
    invoke_semaphore = asyncio.BoundedSemaphore(max_concurrent_tasks)
    for payload in payloads:
        yield _invoke(function_name, payload, session, invoke_semaphore, is_asynchronous)


def invoke_all(function_name, payloads, max_concurrent_tasks=200, is_asynchronous=False, function_sequence_id=None):
    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    function_sequence_id = function_sequence_id if function_sequence_id else function_name
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
