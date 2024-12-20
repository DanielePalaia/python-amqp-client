from ._pyamqp.client import AMQPClient, SendClient, ReceiveClient
from ._pyamqp.authentication import SASLPlainAuth, SASLAnonymousCredential, SASTokenAuth, SASLAnonymousAuth
from ._pyamqp._connection import Connection
from ._pyamqp.sasl import SASLAnonymousCredential

from ._pyamqp._encode import encode_payload
from ._pyamqp.message import Message, Properties
from ._pyamqp.constants import SenderSettleMode, ReceiverSettleMode
from ._pyamqp.endpoints import Target
from ._pyamqp.constants import TransportType

from ._pyamqp.utils import AMQPTypes

import time

def receive_message():

    print("message received")

def main():
    print("im here")

    _properties = {}
    _properties = {"hostname": "vhost:/"}

    conn = Connection("amqp://localhost:5672/", sasl_credential=SASLAnonymousCredential(), properties=_properties, use_tls=False)
    conn.open()
    INCOMING_WINDOW = 64 * 1024
    OUTGOING_WINDOW = 64 * 1024
    conn.create_session()

    link_properties = {}
    link_properties["paired"] = True

    sender = SendClient("amqp://localhost:5672/", "/management", auth=SASLAnonymousAuth(), properties=_properties, transport_type=TransportType.Amqp, send_settle_mode=SenderSettleMode.Settled, receive_settle_mode=ReceiverSettleMode.First, name="management-link-pair", link_properties=link_properties)

    receiver = ReceiveClient("amqp://localhost:5672/", "/management", auth=SASLAnonymousAuth(), properties=_properties,
                        send_settle_mode=SenderSettleMode.Settled, link_credit=100, receive_settle_mode=ReceiverSettleMode.First,  transport_type=TransportType.Amqp, name="management-link-pair",
                        link_properties=link_properties)

    sender.open(conn)
    receiver.open(conn)

    time.sleep(5)


    message = Message(value=None, properties=Properties(message_id="134234234", reply_to= "$me", subject="DELETE", to="/exchanges/test"))

    sender.send_message(message)

    print("message sent")

    message = receiver.receive_messages_iter()

    #print("message received:" +  str(message))

    time.sleep(10)




if __name__ == '__main__':

  main()