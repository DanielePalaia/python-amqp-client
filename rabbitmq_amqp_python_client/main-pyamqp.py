from . import _pyamqp
from ._pyamqp.client import AMQPClient, SendClient, ReceiveClient
from ._pyamqp.authentication import SASLPlainAuth, SASLAnonymousCredential, SASTokenAuth, SASLAnonymousAuth
from ._pyamqp._connection import Connection
from ._pyamqp.sasl import SASLAnonymousCredential

from ._pyamqp.message import Message, Properties
from ._pyamqp.constants import SenderSettleMode, ReceiverSettleMode
from ._pyamqp.endpoints import Target, Source

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
    conn.create_session(incoming_window=INCOMING_WINDOW,outgoing_window=OUTGOING_WINDOW,)

    link_properties = {}
    link_properties["paired"] = True

    sender = SendClient("vhost:/", Target(address="/management", dynamic=False, timeout=10, dynamic_node_properties=False), auth=SASLAnonymousAuth(), properties=_properties, send_settle_mode=SenderSettleMode.Settled, receive_settle_mode=ReceiverSettleMode.First, client_name="management-link-pair", link_properties=link_properties, settled=True)

    #receiver = ReceiveClient("vhost:/", Source(address="/management", dynamic=False, timeout=10, dynamic_node_properties=False) , auth=SASLAnonymousAuth(), properties=_properties,
    #                    send_settle_mode=SenderSettleMode.Settled, link_credit=100, receive_settle_mode=ReceiverSettleMode.First, client_name="management-link-pair",
    #                    link_properties=link_properties, settled=True)

    #receiver.open(conn)
    sender.open(conn)





    message = Message(value=None, properties=Properties(message_id="134234234", reply_to= "$me", subject="DELETE", to="/exchanges/test"))


    #message_received=receiver.receive_messages_iter()
    print("sending")
    sender.mgmt_request(message, operation="DELETE", operation_type="DELETE", node="/management")

    time.sleep(65)

    print("message sent")



    print("message received:" +  str(message_received))





if __name__ == '__main__':

  main()