import optparse
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import DynamicNodeProperties, SenderOption, LinkOption
from proton._endpoints import Link
from proton._data import PropertyDict, symbol
import asyncio
from threading import Thread
from proton.utils import BlockingConnection
import time

class SenderOption(LinkOption):
    """
    Set at-most-once delivery semantics for message delivery. This is achieved by
    setting the sender link settle mode to :const:`proton.Link.SND_SETTLED`
    (ie pre-settled).
    """

    def apply(self, link: Link) -> None:
        """
        Set the at-most-once delivery semantics on the link.

        :param link: The link on which this option is to be applied.
        :type link: :class:`proton.Link`
        """
        link.source.address = "/exchanges/getting-started-exchange/routing-keysender"
        link.snd_settle_mode = Link.SND_SETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol('paired'): True})
        link.source.dynamic = False

    def test(self, link: Link) -> bool:
        return link.is_sender


class ReceiverOption(LinkOption):
    """
    Set at-most-once delivery semantics for message delivery. This is achieved by
    setting the sender link settle mode to :const:`proton.Link.SND_SETTLED`
    (ie pre-settled).
    """

    def apply(self, link: Link) -> None:
        """
        Set the at-most-once delivery semantics on the link.

        :param link: The link on which this option is to be applied.
        :type link: :class:`proton.Link`
        """
        link.target.address = "/exchanges/getting-started-exchange/routing-keysender"
        #link.source.address = "/management"
        link.snd_settle_mode = Link.SND_SETTLED
        link.rcv_settle_mode = Link.RCV_FIRST
        link.properties = PropertyDict({symbol('paired'): True})
        link.source.dynamic = False

    def test(self, link: Link) -> bool:
        return link.is_receiver

def main():



    #try:
    #    Container(Client("amqp://guest:guest@localhost:5672/")).run()
    #except KeyboardInterrupt:
#    pass

    conn = BlockingConnection("amqp://guest:guest@localhost:5672/")

    sender = conn.create_sender("/exchanges/getting-started-exchange/routing-keysender", options=SenderOption())
    #receiver = conn.create_receiver("/exchanges/getting-started-exchange/routing-keysender", options=ReceiverOption())

    #time.sleep(20)
    kv = {}
    kv["auto_delete"] = False
    kv["durable"] = True
    kv["type"] = 'direct'
    kv["internal"] = False
    kv["arguments"] = {}

    msg_declare_exchange = Message(
        id='48e15ea8-0410-44ae-8c71-b133b5e419a5',
        body=kv,
        reply_to='$me',
        address='/exchanges/getting-started-exchangemessage',
        subject='PUT',
        properties={'id':'48e15ea8-0410-44ae-8c71-b133b5e419a5', 'to':'/exchanges/getting-started-exchangemessage', 'subject':'PUT', 'reply_to':'$me'}
    )


    print("message: " + str(msg_declare_exchange))
    #sender.send(msg_declare_exchange)

    kv = {}
    kv["auto_delete"] = False
    kv["durable"] = True
    kv["arguments"] = {'x-queue-type': 'quorum'}

    msg_declare_queue = Message(
        id='48e15ea8-0410-44ae-8c71-b133b5e419a5',
        body=kv,
        reply_to='$me',
        address='/queues/getting-started-exchangemessage',
        subject='PUT',
        properties={'id':'48e15ea8-0410-44ae-8c71-b133b5e419a5', 'to':'/queues/getting-started-exchangemessage', 'subject':'PUT', 'reply_to':'$me'}
    )



    #print(msg)

    #sender.send(msg_declare_queue)

    msg_2 = Message(
        body='test',
    )


    #sender.send(msg_2)

    #msg = receiver.receive()

    #print("received " + str(msg))




if __name__ == '__main__':
  main()
