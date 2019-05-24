import unittest
from unittest.mock import patch, Mock, MagicMock

from zappa.asgi import ZappaASGIServer, MemoryApplicationInstanceBackend, WebsocketProtocol


class AsgiAdapter:
    def __init__(self, consumer):
        self.consumer = consumer

    def __call__(self, scope):
        return AsgiAdapterProxy(self.consumer, scope)


class AsgiAdapterProxy:
    def __init__(self, consumer, scope):
        self.consumer = consumer
        self.scope = scope

    async def __call__(self, receive, send):
        return await self.consumer(self.scope, receive, send)


class AsgiAdapterNotPicklable:
    def __init__(self, consumer):
        self.consumer = consumer

    def __call__(self, scope):
        async def not_picklable(receive, send):
            return await self.consumer(scope, receive, send)
        return not_picklable


async def legacy_consumer_accept_send(scope, receive, send):
    receive = receive
    send = send
    connect_message = await receive()
    assert connect_message['type'] == 'websocket.connect'
    await send({"type": "websocket.accept"})

    # Wait for the first message
    message = await receive()
    # Echo the message
    await send({'type': 'websocket.send', 'text': 'test'})


class SuspendableAcceptSend:
    def __init__(self, scope):
        self.scope = scope

    def __call__(self, receive, send):
        pass

    def resume(self):
        pass

    def suspend(self):
        pass


class TestASGIServer(unittest.TestCase):
    def test_legacy_consumer_accept_send_and_echo(self):
        application_instance_backend = MemoryApplicationInstanceBackend()
        server1 = ZappaASGIServer(
            AsgiAdapter(legacy_consumer_accept_send),
            application_instance_backend=application_instance_backend
        )
        response = server1.handle({
            "headers": {
                "Host": "abcd.execute-api.eu-west-1.amazonaws.com",
                "Origin": "http://abcd.execute-api.eu-west-1.amazonaws.com",
                "Sec-WebSocket-Key": "",
                "Sec-WebSocket-Version": "13",
                "X-Amzn-Trace-Id": "",
                "X-Forwarded-For": "127.0.0.1",
                "X-Forwarded-Port": "443",
                "X-Forwarded-Proto": "https"
            },
            "multiValueHeaders": {
                "Host": [
                    "abcd.execute-api.eu-west-1.amazonaws.com"
                ],
                "Origin": [
                    "http://abcd.execute-api.eu-west-1.amazonaws.com"
                ],
                "Sec-WebSocket-Key": [
                    ""
                ],
                "Sec-WebSocket-Version": [
                    "13"
                ],
                "X-Amzn-Trace-Id": [
                    "4"
                ],
                "X-Forwarded-For": [
                    "127.0.0.1"
                ],
                "X-Forwarded-Port": [
                    "443"
                ],
                "X-Forwarded-Proto": [
                    "https"
                ]
            },
            "requestContext": {
                "routeKey": "$connect",
                "messageId": None,
                "eventType": "CONNECT",
                "extendedRequestId": "",
                "requestTime": "1/January/2019:12:00:00 +0000",
                "messageDirection": "IN",
                "stage": "dev",
                "connectedAt": 1546344000000,
                "requestTimeEpoch": 1546344000000,
                "identity": {
                    "cognitoIdentityPoolId": None,
                    "cognitoIdentityId": None,
                    "principalOrgId": None,
                    "cognitoAuthenticationType": None,
                    "userArn": None,
                    "userAgent": None,
                    "accountId": None,
                    "caller": None,
                    "sourceIp": "127.0.0.1",
                    "accessKey": None,
                    "cognitoAuthenticationProvider": None,
                    "user": None
                },
                "requestId": "req-1234",
                "domainName": "api-1234.execute-api.eu-west-1.amazonaws.com",
                "connectionId": "conn-1234",
                "apiId": "api-1234"
            },
            "isBase64Encoded": False
        }, {})

        self.assertEquals(200, response['statusCode'])

        # Second request. New server and app. Same MemoryBackend.
        server2 = ZappaASGIServer(
            AsgiAdapter(legacy_consumer_accept_send),
            application_instance_backend=application_instance_backend
        )
        send_handler = WebsocketProtocol.SendHandler(server2.response, None, None, None, None, None, None)
        send_handler.handle_send = MagicMock()
        with patch.object(WebsocketProtocol, 'get_send_handler', return_value=send_handler) as method_mock:
            response = server2.handle({
                "requestContext": {
                    "routeKey": "$default",
                    "messageId": None,
                    "eventType": "MESSAGE",
                    "extendedRequestId": "",
                    "requestTime": "1/January/2019:12:00:00 +0000",
                    "messageDirection": "IN",
                    "stage": "dev",
                    "connectedAt": 1546344000000,
                    "requestTimeEpoch": 1546344000000,
                    "identity": {
                        "cognitoIdentityPoolId": None,
                        "cognitoIdentityId": None,
                        "principalOrgId": None,
                        "cognitoAuthenticationType": None,
                        "userArn": None,
                        "userAgent": None,
                        "accountId": None,
                        "caller": None,
                        "sourceIp": "127.0.0.1",
                        "accessKey": None,
                        "cognitoAuthenticationProvider": None,
                        "user": None
                    },
                    "requestId": "req-1234",
                    "domainName": "api-1234.execute-api.eu-west-1.amazonaws.com",
                    "connectionId": "conn-1234",
                    "apiId": "api-1234"
                },
                "body": "Hi!",
                "isBase64Encoded": False
            }, {})

        send_handler.handle_send.assert_called_once_with('test', False)
        self.assertEquals(200, response['statusCode'])

    def test_legacy_consumer_not_picklable_accept_send(self):
        server = ZappaASGIServer(AsgiAdapterNotPicklable(legacy_consumer_accept_send))
        response = server.handle({
            "headers": {
                "Host": "abcd.execute-api.eu-west-1.amazonaws.com",
                "Origin": "http://abcd.execute-api.eu-west-1.amazonaws.com",
                "Sec-WebSocket-Key": "",
                "Sec-WebSocket-Version": "13",
                "X-Amzn-Trace-Id": "",
                "X-Forwarded-For": "127.0.0.1",
                "X-Forwarded-Port": "443",
                "X-Forwarded-Proto": "https"
            },
            "multiValueHeaders": {
                "Host": [
                    "abcd.execute-api.eu-west-1.amazonaws.com"
                ],
                "Origin": [
                    "http://abcd.execute-api.eu-west-1.amazonaws.com"
                ],
                "Sec-WebSocket-Key": [
                    ""
                ],
                "Sec-WebSocket-Version": [
                    "13"
                ],
                "X-Amzn-Trace-Id": [
                    "4"
                ],
                "X-Forwarded-For": [
                    "127.0.0.1"
                ],
                "X-Forwarded-Port": [
                    "443"
                ],
                "X-Forwarded-Proto": [
                    "https"
                ]
            },
            "requestContext": {
                "routeKey": "$connect",
                "messageId": None,
                "eventType": "CONNECT",
                "extendedRequestId": "",
                "requestTime": "1/Januari/2019:12:00:00 +0000",
                "messageDirection": "IN",
                "stage": "dev",
                "connectedAt": 1546344000000,
                "requestTimeEpoch": 1546344000000,
                "identity": {
                    "cognitoIdentityPoolId": None,
                    "cognitoIdentityId": None,
                    "principalOrgId": None,
                    "cognitoAuthenticationType": None,
                    "userArn": None,
                    "userAgent": None,
                    "accountId": None,
                    "caller": None,
                    "sourceIp": "127.0.0.1",
                    "accessKey": None,
                    "cognitoAuthenticationProvider": None,
                    "user": None
                },
                "requestId": "req-1234",
                "domainName": "abcd.execute-api.eu-west-1.amazonaws.com",
                "connectionId": "conn-1234",
                "apiId": "api-1234"
            },
            "isBase64Encoded": False
        }, {})

        self.assertEquals(200, response['statusCode'])
