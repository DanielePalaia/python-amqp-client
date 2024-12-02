import optparse
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import DynamicNodeProperties, SenderOption, LinkOption
from proton._endpoints import Link
import asyncio
from threading import Thread
from proton.utils import BlockingConnection

class MyAtMostOnce(LinkOption):
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
        link.snd_settle_mode = Link.SND_SETTLED

    def test(self, link: Link) -> bool:
        return link.is_sender

def main():



    #try:
    #    Container(Client("amqp://guest:guest@localhost:5672/")).run()
    #except KeyboardInterrupt:
#    pass

    conn = BlockingConnection("amqp://guest:guest@localhost:5672/")

    prop = {}
    prop["paired"] = "true"
    sender_option = {}
    sender_option["address"] = "/management"
    sender_option["dynamic-address"] = "false",
    sender_option["expiring-policy"] = "link-detached"
    sender_option["expiration-timeout"] = "0",
    sender_option["name"] = "management-link-pair",
    sender_option["settle_mode"] = 1,
    sender_option["requested-receiver-settle-mode"] = 2,
    sender_option["properties"] = prop


    sender = conn.create_sender("/management", options=DynamicNodeProperties(sender_option))
    #receiver = conn.create_receiver("/management", options=DynamicNodeProperties(sender_option))
    kv = {}
    kv["auto_delete"] = "false"
    kv["durable"] = "true"
    kv["type"] = "direct"
    kv["INTERNAL"] = "false"
    kv["arguments"] = {}



    msg = Message(
        id='1',
        body=kv,
        reply_to='$me',
        address='/exchanges/test2',
        subject='PUT')

    print(msg)

    sender.send(msg)








if __name__ == '__main__':
  main()