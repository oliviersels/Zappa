import unittest
from unittest.mock import MagicMock, Mock

from zappa.websockets import WebsocketHandler
from zappa.websockets.channels import WebsocketConsumer


class TestWebsockets(unittest.TestCase):
    def test_websocket_handler_channels_connect(self):
        handler = WebsocketHandler(WebsocketConsumer)
        response = handler.handle({
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

        self.assertDictEqual(response, {
            'statusCode': 200,
            'body': '',
        })

    def test_websocket_handler_channels_message(self):
        ws_instance = WebsocketConsumer(None)
        ws_instance_spy = Mock(wraps=ws_instance)

        def init(send, scope):
            ws_instance.send_handler = send
            ws_instance.scope = scope
            return ws_instance_spy
        handler = WebsocketHandler(init)
        response = handler.handle({
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

        self.assertDictEqual(response, {
            'statusCode': 0,
            'body': '',
        })
        ws_instance_spy.websocket_receive.assert_called_once()
