
class WebsocketConsumer:
    def __init__(self, send_handler, scope=None):
        self.send_handler = send_handler
        self.scope = scope

    def websocket_connect(self, message):
        """
        Called when a WebSocket connection is opened.
        """
        self.connect()

    def connect(self):
        self.accept()

    def accept(self, subprotocol=None):
        """
        Accepts an incoming socket
        """
        self.send_handler.send({"type": "websocket.accept", "subprotocol": subprotocol})

    def websocket_receive(self, message):
        """
        Called when a WebSocket frame is received. Decodes it and passes it
        to receive().
        """
        if "text" in message:
            self.receive(text_data=message["text"])
        else:
            self.receive(bytes_data=message["bytes"])

    def receive(self, text_data=None, bytes_data=None):
        """
        Called with a decoded WebSocket frame.
        """
        pass

    def send(self, text_data=None, bytes_data=None, close=False):
        """
        Sends a reply back down the WebSocket
        """
        if text_data is not None:
            self.send_handler.send({"type": "websocket.send", "text": text_data})
        elif bytes_data is not None:
            self.send_handler.send({"type": "websocket.send", "bytes": bytes_data})
        else:
            raise ValueError("You must pass one of bytes_data or text_data")
        if close:
            self.close(close)

    def close(self, code=None):
        """
        Closes the WebSocket from the server end
        """
        if code is not None and code is not True:
            self.send_handler.send({"type": "websocket.close", "code": code})
        else:
            self.send_handler.send({"type": "websocket.close"})

    def websocket_disconnect(self, message):
        """
        Called when a WebSocket connection is closed. Base level so you don't
        need to call super() all the time.
        """
        self.disconnect(message["code"])

    def disconnect(self, code):
        """
        Called when a WebSocket connection is closed.
        """
        pass
