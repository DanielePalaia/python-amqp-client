import optparse
from ._pyamqp.message import Message
from ._pyamqp.client import AMQPClient, SendClient
from ._pyamqp.authentication import SASLPlainAuth, SASLAnonymousCredential, SASTokenAuth, SASLAnonymousAuth
from ._pyamqp._connection import Connection
import asyncio
from threading import Thread
from proton.utils import BlockingConnection

def main():
    print("im here")

    _properties = {}
    _properties["hostname"] = "/"

    conn = Connection("amqp://guest:guest@localhost:5672/", properties=_properties, auth=SASLAnonymousAuth())
    conn.open()


    #client = AMQPClient("amqp://guest:guest@localhost:5672/", auth=SASLAnonymousAuth())
    #client.open()



    #client = SendClient("amqp://guest:guest@localhost:5672/", "/management", auth=SASLAnonymousAuth())

    #client.open()




if __name__ == '__main__':

  main()