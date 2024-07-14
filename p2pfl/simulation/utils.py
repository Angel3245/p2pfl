#
# This file is part of the federated_learning_p2p (p2pfl) distribution
# (see https://github.com/pguijas/federated_learning_p2p).
# Copyright (c) 2022 Pedro Guijas Bravo.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

from typing import Dict, Union
import ray

from p2pfl.management.logger import logger

def pool_size_from_resources(client_resources: Dict[str, Union[int, float]]) -> int:
    """Calculate number of Actors that fit in the cluster.

    For this we consider the resources available on each node and those required per
    client.
    """
    total_num_actors = 0

    # We calculate the number of actors that fit in a node per node basis. This is
    # the right way of doing it otherwise situations like the following arise: imagine
    # each client needs 3 CPUs and Ray has w nodes (one with 2 CPUs and another with 4)
    # if we don't follow a per-node estimation of actors, we'll be creating an actor
    # pool with 2 Actors. This, however, doesn't fit in the cluster since only one of
    # the nodes can fit one Actor.
    nodes = ray.nodes()
    for node in nodes:
        node_resources = node["Resources"]

        # If a node has detached, it is still in the list of nodes
        # however, its resources will be empty.
        if not node_resources:
            continue

        num_cpus = node_resources["CPU"]
        num_gpus = node_resources.get("GPU", 0)  # There might not be GPU
        num_actors = int(num_cpus / client_resources["num_cpus"])

        # If a GPU is present and client resources do require one
        if "num_gpus" in client_resources.keys() and client_resources["num_gpus"] > 0.0:
            if num_gpus:
                # If there are gpus in the cluster
                num_actors = min(
                    num_actors, int(num_gpus / client_resources["num_gpus"])
                )
            else:
                num_actors = 0
        total_num_actors += num_actors

    if total_num_actors == 0:
        logger.debug(
            "localhost",
            f"The ActorPool is empty. The system (CPUs={num_cpus}, GPUs={num_gpus})"+
            "does not meet the criteria to host at least one client with resources:"+
            "{client_resources}. Lowering the `client_resources` could help."
        )
        raise ValueError(
            "ActorPool is empty. Stopping Simulation. "
            "Check 'client_resources' passed to `start_simulation`"
        )

    return total_num_actors