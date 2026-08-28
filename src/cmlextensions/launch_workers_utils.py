# Copyright 2026 Cloudera. All Rights Reserved.
#
# This file is licensed under the Apache License Version 2.0
# (the "License"). You may not use this file except in compliance
# with the License. You may obtain  a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0.
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
# License for the specific permissions and limitations governing your
# use of the file.

import inspect
import warnings

from cmlextensions.rdma_network_utils import resolve_rdma_network_selections


def add_rdma_launch_args(
    args,
    launch_workers_fn,
    rdma_network_selections=None,
    list_labels_fn=None,
):
    """Add resolved RDMA network selections to launch_workers args when supported."""
    if rdma_network_selections is None:
        return args

    params = inspect.signature(launch_workers_fn).parameters
    if "rdma_network_selections" not in params:
        warnings.warn(
            "rdma_network_selections was provided but this CML workspace's "
            "launch_workers API does not support it; RDMA network settings "
            "will be ignored. Upgrade your Cloudera AI Workbench to use RDMA networks.",
            UserWarning,
            stacklevel=2,
        )
        return args

    resolved_selections = resolve_rdma_network_selections(
        rdma_network_selections,
        list_labels_fn=list_labels_fn,
    )
    if resolved_selections is not None:
        args["rdma_network_selections"] = resolved_selections
    return args
