from datetime import datetime
import random
from typing import Any, Callable, List, Optional, Union
from p2pfl.commands.command import Command
from p2pfl.communication.gossiper import Gossiper
from p2pfl.communication.heartbeater import Heartbeater
from p2pfl.communication.simulation.client import RayClient
from p2pfl.communication.simulation.neighbors import RayNeighbors
from p2pfl.settings import Settings
import ray

from p2pfl.communication.communication_protocol import CommunicationProtocol, ModelFunction, StatusFunction

class RayCommunicationProtocol(CommunicationProtocol):

    def __init__(self, addr: str = None, commands: List[Command] = []) -> None:
        self.addr = addr
        self.commands = commands

        # Ray Client
        self._client = RayClient(self.addr, self._neighbors)
        self._neighbors = RayNeighbors(ray.get(self.actor.get_address.remote()))

        # Gossip
        self._gossiper = Gossiper(self.addr, self._client)
        # Hearbeat
        self._heartbeater = Heartbeater(self.addr, self._neighbors, self._client)

    def start(self) -> None:
        self._heartbeater.start()
        self._gossiper.start()

    def stop(self) -> None:
        self._heartbeater.stop()
        self._gossiper.stop()
        self._neighbors.clear_neighbors()

    def add_command(self, cmds: Union[Command, List[Command]]) -> None:
        if isinstance(cmds, list):
            self.commands.extend(cmds)
        else:
            self.commands.append(cmds)

    def build_msg(self, cmd: str, args: List[str] = [], round: Optional[int] = None) -> dict:
        return self._client.build_message(cmd, args, round)

    def build_weights(self,
        cmd: str,
        round: int,
        serialized_model: bytes,
        contributors: Optional[List[str]] = [],
        weight: int = 1,) -> dict:
        return self._client.build_weights(
            cmd, round, serialized_model, contributors, weight
        )

    def send(self, nei: str, message: Any, node_list: Optional[List[str]] = None) -> None:
        self._client.send(nei, message, create_connection=True)

    def broadcast(self, message: Any, node_list: Optional[List[str]] = None) -> None:
        self._client.broadcast(message, node_list)

    def connect(self, addr: str, non_direct: bool = False) -> None:
        return self._neighbors.add(addr, non_direct=non_direct)

    def disconnect(self, nei: str, disconnect_msg: bool = True) -> None:
        self._neighbors.remove(nei, disconnect_msg=disconnect_msg)

    def get_neighbors(self) -> List[str]:
        return list(self._neighbors.neis)

    def get_address(self) -> str:
        return self.addr

    def wait_for_termination(self) -> None:
        pass  # Implementation depends on how you want to handle termination

    def gossip_weights(
        self,
        early_stopping_fn: Callable[[], bool],
        get_candidates_fn,
        status_fn: StatusFunction,
        model_fn: ModelFunction,
        period: float = Settings.GOSSIP_MODELS_PERIOD,
        create_connection: bool = False,
    ) -> None:
        self._gossiper.gossip_weights(
            early_stopping_fn,
            get_candidates_fn,
            status_fn,
            model_fn,
            period,
            create_connection,
        )
