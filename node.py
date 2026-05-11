import asyncio
import json
import os
from aiocoap import Context, Message, GET, PUT, CON
from constants import COAP_GET_TIMEOUT, COAP_PUT_TIMEOUT, LIVE_PERIOD, SYNC_PERIOD
#Class to interact with Node
class Node:
    #Initialise the ipv6, device uri, payload
    def __init__(self, ipv6: str, uri: str, payload: str = ""):
        self.ipv6 = ipv6
        self.uri = uri
        self.payload = payload.encode() if isinstance(payload, str) else payload
    #Sending a GET Request to Device
    async def get(self, timeout=COAP_GET_TIMEOUT):
        return await self._request(GET, timeout)

    #Sending a PUT Request to Device
    async def put(self, timeout=COAP_PUT_TIMEOUT):
        return await self._request(PUT, timeout, CON)
    #General Request Function
    async def _request(self, code, timeout, mtype=None):
        #Creating a UDP Socket for CoAP Request
        protocol = await Context.create_client_context()
        req = Message(code=code, uri=self._uri(), payload=self.payload)
        if mtype:
            req.mtype = mtype
            req.token = os.urandom(2)

        try:
            resp = await asyncio.wait_for(protocol.request(req).response, timeout)
            return self._parse(resp)
        except asyncio.TimeoutError:
            print(f"Timeout: {self.ipv6}/{self.uri}")
            return None
        #Handle the COAP Exception like co-retransmission, etc..
        except Exception as e:
            print(f"CoAP Error {self.ipv6}: {e}")
            return None

    #Function to build the Coap Request with IP, URI
    def _uri(self):
        return f"coap://[{self.ipv6}]:5683/{self.uri}"

    @staticmethod #Parsing method for payload
    def _parse(resp):
        if not resp or not resp.payload:
            return None
        text = resp.payload.decode().strip()
        try:
            print("Data : ", text)
            return json.loads(text)
        except:
            return text
