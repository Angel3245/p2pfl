from typing import List, Optional, Union
import random
from datetime import datetime
import ray

from p2pfl.management.logger import logger
from p2pfl.settings import Settings

class NeighborNotConnectedError(Exception):
    pass

class RayClient:
    """
    Implementation of the client side (i.e. who initiates the communication) using Ray Actors.
    """

    def __init__(self, self_addr: str, neighbors: dict) -> None:
        self.__self_addr = self_addr
        self.__neighbors = neighbors

    ####
    # Message Building
    ####

    def build_message(self, cmd: str, args: Optional[List[str]] = None, round: Optional[int] = None):
        """
        Build a message to send to the neighbors.

        Args:
            cmd (string): Command of the message.
            args (list): Arguments of the message.
            round (int): Round of the message.

        Returns:
            dict: Message to send.
        """
        if round is None:
            round = -1
        if args is None:
            args = []
        hs = hash(
            str(cmd) + str(args) + str(datetime.now()) + str(random.randint(0, 100000))
        )
        args = [str(a) for a in args]

        return {
            'source': self.__self_addr,
            'ttl': Settings.TTL,
            'hash': hs,
            'cmd': cmd,
            'args': args,
            'round': round,
        }

    def build_weights(self, cmd: str, round: int, serialized_model: bytes, contributors: Optional[List[str]] = [], weight: int = 1):
        """
        Build a weight message to send to the neighbors.

        Args:
            cmd (str): Command of the message.
            round (int): Round of the model.
            serialized_model (bytes): Serialized model.
            contributors (list): List of contributors of the model.
            weight (float): Weight of the model.

        Returns:
            dict: Weights message to send.
        """
        return {
            'source': self.__self_addr,
            'round': round,
            'weights': serialized_model,
            'contributors': contributors,
            'weight': weight,
            'cmd': cmd,
        }

    ####
    # Message Sending
    ####

    def send(self, nei: str, msg: Union[dict, dict], create_connection: bool = False) -> None:
        try:
            # Get neighbor
            if nei not in self.__neighbors and not create_connection:
                raise NeighborNotConnectedError(f"Neighbor {nei} not found.")

            node_stub = self.__neighbors[nei]

            # Send message
            if isinstance(msg, dict) and 'weights' in msg:
                res = ray.get(node_stub.receive_weights.remote(msg))
            else:
                res = ray.get(node_stub.receive_message.remote(msg))

            if res.get('error'):
                logger.error(
                    self.__self_addr,
                    f"Error while sending a message: {msg['cmd']} {msg['args']}: {res['error']}",
                )
                self.__neighbors.pop(nei, None)
        except Exception as e:
            # Remove neighbor
            logger.info(
                self.__self_addr,
                f"Cannot send message {msg['cmd']} to {nei}. Error: {str(e)}",
            )
            self.__neighbors.pop(nei, None)

    def broadcast(self, msg: dict, node_list: Optional[List[str]] = None) -> None:
        """
        Broadcast a message to all the neighbors.

        Args:
            msg (dict): Message to send.
            node_list (list): List of neighbors to send the message. If None, send to all the neighbors.
        """
        # Node list
        if node_list is None:
            node_list = list(self.__neighbors.keys())

        # Send
        for n in node_list:
            self.send(n, msg)