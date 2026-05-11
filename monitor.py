import re
from pydbus import SystemBus
from node import Node

class WiSunMonitor:
    def __init__(self):
        self.bus = SystemBus()
        self.proxy = self.bus.get("com.silabs.Wisun.BorderRouter", "/com/silabs/Wisun/BorderRouter")
        self.initial_mapping_done = False

    @staticmethod
    def slice_ipv6(source):
        return [source[i: i + 4] for i in range(0, len(source), 4)]

    @staticmethod
    def pretty_ipv6(ipv6):
        ipv6 = ":".join(WiSunMonitor.slice_ipv6(ipv6))
        ipv6 = re.sub("0000:", ":", ipv6)
        ipv6 = re.sub(":{2,}", "::", ipv6)
        return ipv6
        
    def is_ipv6(self, address):
        ipv6_pattern = r'^([0-9a-fA-F]{1,4}:){1,7}([0-9a-fA-F]{1,4}|:)$|^([0-9a-fA-F]{1,4}:)*::$|^::$'
        return bool(re.match(ipv6_pattern, address)) or '::' in address
        
    async def get_nodes(self):
        global connected_nodes, gateway_name
        try:
            nodes = await self.proxy.Nodes if "ipv6" in self.proxy.Nodes[0][1] else self.proxy.RoutingGraph
            result = set()
            if "ipv6" in self.proxy.Nodes[0][1]:
                for node in nodes:
                    ipv6 = bytes(node[1]["ipv6"][1]).hex()
                    result.add(self.pretty_ipv6(ipv6))
            else:
                for node in nodes[1:]:
                    ipv6 = bytes(node[0]).hex()
                    result.add(self.pretty_ipv6(ipv6))
            connected_nodes = result
            return tuple(result)
        except Exception as e:
            print(f"WARNING: Exception error fetching nodes: {e}")
            return []
