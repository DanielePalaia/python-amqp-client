from ._pyamqp.client import AMQPClient, SendClient, ReceiveClient
from ._pyamqp.authentication import SASLPlainAuth, SASLAnonymousCredential, SASTokenAuth, SASLAnonymousAuth
from ._pyamqp._connection import Connection
from ._pyamqp.sasl import SASLAnonymousCredential

from ._pyamqp._encode import encode_payload
from ._pyamqp.message import Message, Properties
from ._pyamqp.constants import SenderSettleMode, ReceiverSettleMode
from ._pyamqp.endpoints import Target

from ._pyamqp.utils import AMQPTypes

import time


def main():
    print("im here")

    _properties = {}
    _properties = {"hostname": "vhost:/"}

    conn = Connection("amqp://localhost:5672/", sasl_credential=SASLAnonymousCredential(), properties=_properties, use_tls=False)
    conn.open()

    link_properties = {}
    link_properties["paired"] = True

    sender = SendClient("amqp://guest:guest@localhost:5672/", Target(address="/management", dynamic=False), auth=SASLAnonymousAuth(), properties=_properties, custom_endpoint_address="/management", send_settle_mode=SenderSettleMode.Settled, receive_settle_mode=ReceiverSettleMode.First, name="management-link-pair", link_properties=link_properties)

    sender.open(conn)

    receiver = ReceiveClient("amqp://guest:guest@localhost:5672/", Target(address="/management", dynamic=False), auth=SASLAnonymousAuth(), properties=_properties, custom_endpoint_address="/management",
                        send_settle_mode=SenderSettleMode.Settled, receive_settle_mode=ReceiverSettleMode.First, name="management-link-pair",
                        link_properties=link_properties)

    receiver.open(conn)

    message = Message(value=None, properties=Properties(message_id="134234234", reply_to= "$me", subject="DELETE", to="/exchanges/test"))

    sender.send_message(message)

    message = receiver.receive_messages_iter()

    print("message received:" +  str(message))

    time.sleep(10)




if __name__ == '__main__':

  main()