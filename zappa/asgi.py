import asyncio
import logging
import pickle
import urllib.parse
from base64 import b64decode

import boto3
import requests
from asgiref.sync import sync_to_async
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

logger = logging.getLogger(__name__)


class ApplicationInstanceBackendError(Exception):
    pass


class BaseApplicationInstanceBackend:
    """
    Backend to store and retrieve ASGI application instances.

    When using AWS websockets the ASGI application instance needs to be stored after the first CONNECT request so
    subsequent MESSAGE and DISCONNECT requests can retrieve and continue the ASGI application.
    This backend provides functionality to store and retrieve these instances.
    """

    def get(self, connection_id):
        data = self._get(connection_id)
        if data is None:
            return data
        try:
            return pickle.loads(data)
        except (pickle.UnpicklingError, AttributeError) as e:
            raise ApplicationInstanceBackendError() from e

    def _get(self, connection_id):
        """
        Return the raw data from the backend or None if not found.
        """
        raise NotImplementedError('subclasses of BaseApplicationInstanceBackend must provide a _get() method')

    def set(self, connection_id, application_instance):
        try:
            data = pickle.dumps(application_instance)
        except (pickle.PicklingError, AttributeError, TypeError) as e:
            raise ApplicationInstanceBackendError() from e
        else:
            self._set(connection_id, data)

    def _set(self, connection_id, data):
        raise NotImplementedError('subclasses of BaseApplicationInstanceBackend must provide a _set() method')

    def delete(self, connection_id):
        self._delete(connection_id)

    def _delete(self, connection_id):
        raise NotImplementedError('subclasses of BaseApplicationInstanceBackend must provide a _delete() method')


class MemoryApplicationInstanceBackend(BaseApplicationInstanceBackend):
    def __init__(self):
        self.storage = {}

    def _get(self, connection_id):
        return self.storage.get(connection_id, None)

    def _set(self, connection_id, data):
        self.storage[connection_id] = data

    def _delete(self, connection_id):
        del self.storage[connection_id]


def extract_headers(event):
    """
    Returns the headers from the request

    The format is: a dict of {name: value_list} where value_list is an iterable of all header values.

    Supports regular and multivalue headers.
    """
    headers = event.get('headers') or {}
    multi_headers = (event.get('multiValueHeaders') or {}).copy()
    for h in set(headers.keys()):
        if h not in multi_headers:
            multi_headers[h] = [headers[h]]

    return multi_headers


def extract_query_string_parameters(event):
    """
    Returns the query string parameters from the request

    The format is: a sequence of two-element tuples (name, value)

    Supports regular and multivalue querystringparameters
    :param event:
    :return:
    """
    query_string_params = event.get("queryStringParameters", {})
    multi_query_string_params = event.get("multiValueQueryStringParameters", {})
    for qsp in query_string_params.keys():
        if qsp not in multi_query_string_params:
            multi_query_string_params[qsp] = [query_string_params[qsp]]

    result = []
    for name, value_list in multi_query_string_params.items():
        for value in value_list:
            result.append((name, value))
    return result


def asgi_headers(headers):
    """
    Returns the headers in ASGI format

    The format is: an iterable of [name, value] two-item iterables.
    """
    asgi_headers = []
    for name, value_list in headers.items():
        for value in value_list:
            asgi_headers.append((name, value))
    return asgi_headers


class WebsocketProtocol:
    def __init__(self, server):
        self.server = server
        self.connection_id = None
        self.api_id = None
        self.stage = None
        self.connecting = False
        self.closing = False

    def handle(self, event, context):
        self.connection_id = event['requestContext']['connectionId']
        self.api_id = event['requestContext']['apiId']
        self.stage = event['requestContext']['stage']
        event_type = event['requestContext']['eventType']
        if event_type == 'CONNECT':
            self.connecting = True
            scope = self.create_initial_scope(event, context)
            aborting_receive = self.server.create_application(self, scope)
            connect_message = {"type": "websocket.connect"}
            aborting_receive.queue.put_nowait(connect_message)
            aborting_receive.abort_when_empty()
        elif event_type == 'MESSAGE':
            aborting_receive = self.server.create_application(self, {})
            self.resume_application(aborting_receive.queue)
            if event['isBase64Encoded']:
                receive_message = {"type": "websocket.receive", "bytes": b64decode(event['body'])}
            else:
                receive_message = {"type": "websocket.receive", "text": event['body']}
            aborting_receive.queue.put_nowait(receive_message)
            aborting_receive.abort_when_empty()
        elif event_type == 'DISCONNECT':
            aborting_receive = self.server.create_application(self, {})
            self.resume_application(aborting_receive.queue)
            aborting_receive.queue.put_nowait({"type": "websocket.disconnect"})
            # AWS gateway does not pass the disconnect code.
            # aborting_receive.queue.put_nowait({"type": "websocket.disconnect", "code": code})
            aborting_receive.abort_when_empty()

    def resume_application(self, receive_queue):
        if hasattr(self.application_instance, 'resume'):
            self.application_instance.resume()
        else:
            # This is a default application. Send connect again before the message.
            connect_message = {"type": "websocket.connect"}
            receive_queue.put_nowait(connect_message)

    def get_or_create_application_instance(self, application, scope):
        try:
            self.application_instance = ZappaASGIServer.application_instance_backend.get(self.connection_id)
        except ApplicationInstanceBackendError:
            logger.warning('Error retrieving application instance.', exc_info=True)
            self.application_instance = None

        if self.application_instance:
            # TODO: Resume call
            pass
        elif scope:
            self.application_instance = application(scope=scope)
        return self.application_instance

    def suspend_application_instance(self):
        # TODO: Suspend call
        try:
            ZappaASGIServer.application_instance_backend.set(self.connection_id, self.application_instance)
        except ApplicationInstanceBackendError:
            logger.warning('Could not suspend application instance: %s', self.application_instance, exc_info=True)

    def create_initial_scope(self, event, context):
        headers = extract_headers(event)
        scheme = headers.get("X-Forwarded-Proto", ["ws"])[0]
        scheme = scheme.replace('http', 'ws')
        path = event.get("path", '/')

        query_string_params = extract_query_string_parameters(event)
        query_string = (
            urllib.parse.urlencode(query_string_params).encode()
            if query_string_params
            else b''
        )

        client_addr = event["requestContext"].get("identity", {}).get("sourceIp", None)
        client = (client_addr, 0)

        server_addr = headers.get("Host", [None])[0]
        if server_addr is not None:
            if ":" not in server_addr:
                server_port = 80
            else:
                server_port = int(server_addr.split(":")[1])

            server = (server_addr, server_port)
        else:
            server = None  # pragma: no cover

        return {
            "type": "websocket",
            "asgi": {
                "version": "3.0",
                "spec_version": "2.1",
            },
            "http_version": "1.1",
            "scheme": scheme,
            "path": path,
            "query_string": query_string,
            "root_path": "",
            "headers": asgi_headers(headers),
            "client": client,
            "server": server,
            "extensions": {
                "aws.lambda": {
                    "event": event,
                    # "context": context,  # Not picklable
                }
            }
        }

    def get_send_handler(self):
        return self.SendHandler(
            self.server.response,
            self.connecting,
            self.connection_id,
            self.api_id,
            self.server.region,
            self.stage,
        )

    class SendHandler:
        def __init__(self, response, connecting, connection_id, api_id, region, stage):
            self.response = response
            self.connecting = connecting
            self.connection_id = connection_id
            self.api_id = api_id
            self.region = region
            self.stage = stage

        async def __call__(self, message):
            return await self.handle_reply(message)

        async def handle_reply(self, message):
            if 'type' not in message:
                raise ValueError('Message has no type defined')
            if message['type'] == 'websocket.accept':
                self.handle_accept(message.get('subprotocol', None), message.get('headers', []))
            elif message['type'] == 'websocket.close':
                if self.connecting:
                    self.handle_reject()
                else:
                    await self.handle_close()
            elif message['type'] == 'websocket.send':
                if self.connecting:
                    # Sending messages is not possible during the @connect event.
                    # Ignore this because some default ASGI apps might not know this.
                    logger.warning('Trying to send a message during connect event. This is not possible in AWS.')
                    return
                if message.get("bytes", None) and message.get("text", None):
                    raise ValueError(
                        "Got invalid WebSocket reply message on %s - contains both bytes and text keys"
                        % (message,)
                    )
                if message.get("bytes", None):
                    await self.handle_send(message["bytes"], True)
                if message.get("text", None):
                    await self.handle_send(message["text"], False)

        def handle_accept(self, subprotocol, headers):
            if not self.connecting:
                # Default ASGI consumers might try to connect after a resume. Ignore this.
                return

            self.connecting = False
            if headers is None:
                headers = []
            if subprotocol:
                headers.append(['sec-websocket-protocol', subprotocol])
            self.response.status = 200

            if headers:
                self.response.headers = headers

        def handle_reject(self):
            self.response.status = 403

        async def handle_close(self):
            await self.send_close()
            ZappaASGIServer.application_instance_backend.delete(self.connection_id)

        async def handle_send(self, body, binary=False):
            await self.send_message(body, binary)

        async def send_close(self):
            logger.info('Closing connection: %s', self.get_connections_url())
            response = await sync_to_async(requests.delete)(
                self.get_connections_url(),
                auth=self.get_connections_auth(),
                timeout=1
            )
            if response.status_code != 410:
                response.raise_for_status()

        def get_connections_auth(self):
            return BotoAWSRequestsAuth(
                aws_host='{api_id}.execute-api.{region}.amazonaws.com'.format(
                    api_id=self.api_id,
                    region=self.region
                ),
                aws_region=self.region,
                aws_service='execute-api'
            )

        def get_connections_url(self):
            return 'https://{api_id}.execute-api.{region}.amazonaws.com/{stage}/@connections/{connection_id}'.format(
                api_id=self.api_id,
                region=self.region,
                stage=self.stage,
                connection_id=self.connection_id
            )

        async def send_message(self, body, binary):
            response = await sync_to_async(requests.post)(
                self.get_connections_url(),
                auth=self.get_connections_auth(),
                data=body,
                timeout=1)
            response.raise_for_status()


class HttpProtocol:
    def __init__(self, server):
        self.server = server

    def handle(self, event, context):
        scope = self.create_http_scope(event, context)
        # TODO: Implement logic from mangum

    def get_send_handler(self):
        return self.handle_reply

    async def handle_reply(self, message):
        pass

    def create_http_scope(self, event, context):
        headers = extract_headers(event)
        method = event["httpMethod"]
        path = event["path"]
        scheme = headers.get("X-Forwarded-Proto", ["http"])[0]
        query_string_params = extract_query_string_parameters(event)
        query_string = (
            urllib.parse.urlencode(query_string_params).encode()
            if query_string_params
            else b''
        )

        client_addr = event["requestContext"].get("identity", {}).get("sourceIp", None)
        client = (client_addr, 0)
        server_addr = headers.get("Host", [None])[0]
        if server_addr is not None:
            if ":" not in server_addr:
                server_port = 80
            else:
                server_port = int(server_addr.split(":")[1])

            server = (server_addr, server_port)
        else:
            server = None  # pragma: no cover

        return {
            "server": server,
            "client": client,
            "scheme": scheme,
            "root_path": "",
            "query_string": query_string,
            "headers": asgi_headers(headers),
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "path": path,
            "extensions": {
                "aws.lambda": {
                    "event": event,
                    "context": context,
                }
            }
        }


class AbortedReceiveException(Exception):
    pass


class AbortingReceive:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.abort = False
        self.receive_task = None

    async def receive(self):
        if self.abort and self.queue.empty():
            raise AbortedReceiveException()

        try:
            self.receive_task = asyncio.ensure_future(self.queue.get())
            return await self.receive_task
        except asyncio.CancelledError:
            return await self.receive()  # Should trigger the abort exception

    def abort_when_empty(self):
        self.abort = True
        if self.receive_task:
            self.receive_task.cancel()


class AwsResponse:
    def __init__(self):
        self.status = 0
        self.body = ''
        self.headers = {}

    def get_aws_response(self):
        response = {
            'statusCode': self.status,
            'body': self.body,
        }
        if self.headers:
            response['headers'] = self.headers
        return response


class ZappaASGIServer:
    """
    The server translates between the AWS websocket protocol and the ASGI spec.

    Note that it is not possible to fully map the AWS websocket protocol to the ASGI spec. This can result in standard
    ASGI apps misbehaving (although it will work in most cases). An ASGI extension is used to notify applications that
    they are running in this AWS version of ASGI so they can take measures to be compatible.

    Why can't AWS websockets be mapped to the ASGI spec?
    The ASGI spec expects an application to exist for as long as the connection is active. The application then uses an
    asynchronous `receive` and `send` to communicate with the outside world. In the context of AWS API Gateway and
    Lambda this is not the case and cannot be emulated. API gateway keeps the websocket connection open and emits each
    incoming message as a new event to Lambda. This means applications are only active for the handling of a single
    websocket message. For http requests this is fine, but not for actual websocket connections.

    What does the extension do?
    The ASGI extension signals to the application that it will only exist for as long as the current message is handled.
    The application can then react accordingly and change its behavior.

    This implementation is based on:
     - the `StatelessServer` in asgiref.server
     - mangum
     - the websocket implementation in daphne (in channels for django)
    """
    application_instance_backend = MemoryApplicationInstanceBackend()

    def __init__(self, application, region=None):
        self.application = application
        if not region:
            region = boto3.session.Session().region_name
        self.region = region
        self.loop = None
        self.application_task = None
        self.response = AwsResponse()

    def handle(self, event, context):
        self.loop = asyncio.get_event_loop()
        if 'httpMethod' in event:
            protocol = HttpProtocol(self)
        elif 'requestContext' in event and 'connectionId' in event['requestContext']:
            protocol = WebsocketProtocol(self)
        else:
            raise ValueError('Can\'t handle event')

        protocol.handle(event, context)
        try:
            self.loop.run_until_complete(self.application_task)
        except AbortedReceiveException:
            # The application has tried to receive data which isn't there. Continue as normal.
            pass

        if not self.response.status:
            self.response.status = 200

        protocol.suspend_application_instance()
        return self.response.get_aws_response()

    def create_application(self, protocol, scope):
        application_instance = protocol.get_or_create_application_instance(self.application, scope)
        send = protocol.get_send_handler()
        aborting_receive = AbortingReceive()
        if not application_instance:
            self.application_task = asyncio.ensure_future(
                send({'type': 'websocket.close'})
            )
        else:
            self.application_task = asyncio.ensure_future(
                application_instance(
                    receive=aborting_receive.receive,
                    send=send,
                ),
                loop=self.loop)
        return aborting_receive
