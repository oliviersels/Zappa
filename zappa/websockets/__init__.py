
import logging
import urllib.parse
from base64 import b64decode

import boto3
import botocore.exceptions

logger = logging.getLogger(__name__)


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


class WebsocketHandler:
    def __init__(self, app):
        self.app = app

    def handle(self, event, context):
        connection_id = event['requestContext']['connectionId']
        api_id = event['requestContext']['apiId']
        stage = event['requestContext']['stage']
        send_handler = self.send_handler(connection_id, api_id, stage)

        event_type = event['requestContext']['eventType']
        if event_type == 'CONNECT':
            scope = self.create_initial_scope(event, context)
            app_instance = self.app(send_handler, scope)
            app_instance.websocket_connect({'type': 'websocket.connect'})
        elif event_type == 'MESSAGE':
            send_handler.response.status = 200
            app_instance = self.app(send_handler, None)
            if event['isBase64Encoded']:
                app_instance.websocket_receive({'type': 'websocket.receive', 'bytes': b64decode(event['body'])})
            else:
                app_instance.websocket_receive({'type': 'websocket.receive', 'text': event['body']})
        elif event_type == 'DISCONNECT':
            send_handler.response.status = 200
            app_instance = self.app(send_handler, None)
            app_instance.websocket_disconnect({'type': 'websocket.disconnect', 'code': 1000})  # TODO: Get from event?
        else:
            logger.error('Unexpected websocket event type: %s', event_type)
            return send_handler.send({'type': 'websocket.close', 'code': 1001})  # 1003 Going Away

        return send_handler.response.get_aws_response()

    def send_handler(self, connection_id, api_id, stage):
        return SendHandler(connection_id, api_id, stage)

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
                    "context": context,
                }
            }
        }


class AwsResponse:
    def __init__(self, status=0):
        self.status = status
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


class SendHandler:
    def __init__(self, connection_id, api_id, stage):
        self.connection_id = connection_id
        self.session = boto3.Session()
        self.client = self.session.client('apigatewaymanagementapi', endpoint_url=f'https://{api_id}.execute-api.{self.session.region_name}.amazonaws.com/{stage}')
        self.response = AwsResponse()

    def send(self, event):
        """
        Event is an event from the ASGI Websocket spec
        """
        if 'type' not in event:
            raise ValueError('Event is not valid: type not provided. Should be conform the ASGI Websocket spec.')

        if event['type'] == 'websocket.accept':
            self.response.status = 200
            if event.get('subprotocol', None):
                self.response.headers['sec-websocket-protocol'] = event['subprotocol']
        elif event['type'] == 'websocket.send':
            data = event.get('bytes', None)
            if not data:
                data = event.get('text', '').encode('utf-8')
            self._send_to_client(data)
        elif event['type'] == 'websocket.close':
            code = event.get('code', 1000)
            self._close(code)
            self.response.status = 403
        else:
            raise ValueError('Event is not valid: unknown type. Should be conform the ASGI Websocket spec.')

    def _close(self, code):
        try:
            self.client.delete_connection(ConnectionId=self.connection_id)
        except self.client.exceptions.GoneException:
            pass  # Ignore GoneExceptions, can't close a connection when there is no client anymore.

    def _send_to_client(self, data):
        self.client.post_to_connection(
            ConnectionId=self.connection_id,
            Data=data,
        )
