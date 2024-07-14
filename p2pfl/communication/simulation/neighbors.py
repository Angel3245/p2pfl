from typing import Any
import ray
from p2pfl.management.logger import logger
from p2pfl.communication.neighbors import Neighbors

class RayNeighbors(Neighbors):
    def __init__(self, self_addr) -> None:
        super().__init__(self_addr)

    def connect(self, addr: str) -> Any:
        try:
            actor = ray.get_actor(addr)
            self.neis[addr] = (actor, True)
            logger.info(self.self_addr, f"Connected to {addr}")
            return actor
        except Exception as e:
            logger.error(self.self_addr, f"Error connecting to {addr}: {e}")
            raise

    def disconnect(self, addr: str) -> None:
        if addr in self.neis:
            del self.neis[addr]
            logger.info(self.self_addr, f"Disconnected from {addr}")