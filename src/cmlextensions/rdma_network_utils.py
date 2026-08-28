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

RDMA_NETWORK_LABEL_KEY = "rdma.network.name"


class RdmaNetworkSelectionError(ValueError):
    """Raised when RDMA network selections are invalid."""


def _get_field(label, *names):
    if isinstance(label, dict):
        for name in names:
            if name in label and label[name] is not None:
                return label[name]
        return None

    for name in names:
        if hasattr(label, name):
            value = getattr(label, name)
            if value is not None:
                return value
    return None


def normalize_node_label(label):
    return {
        "id": _get_field(label, "id"),
        "label_key": _get_field(label, "label_key", "labelKey"),
        "label_value": _get_field(label, "label_value", "labelValue"),
        "display_name": _get_field(label, "display_name", "displayName"),
        "availability": _get_field(label, "availability"),
    }


def list_node_labels_from_cmlapi():
    try:
        import cmlapi
    except ImportError as error:
        raise RdmaNetworkSelectionError(
            "cmlapi is required to resolve RDMA network labels by name. "
            "Install cmlapi or run this code from a CML session."
        ) from error

    client = cmlapi.default_client()

    if hasattr(client, "list_all_accelerator_node_labels"):
        response = client.list_all_accelerator_node_labels()
        labels = _get_field(response, "accelerator_node_label", "accelerator_node_labels") or []
        if labels:
            return labels

    if hasattr(client, "list_node_labels"):
        response = client.list_node_labels()
        return _get_field(response, "node_labels", "nodeLabels") or []

    raise RdmaNetworkSelectionError(
        "Could not list node labels from cmlapi. "
        "Expected list_all_accelerator_node_labels() or list_node_labels()."
    )


def _available_rdma_network_labels(labels):
    rdma_labels = []
    for label in labels:
        normalized = normalize_node_label(label)
        if normalized["label_key"] != RDMA_NETWORK_LABEL_KEY:
            continue
        if normalized["availability"] is False:
            continue
        rdma_labels.append(normalized)
    return rdma_labels


def _build_rdma_label_lookup(rdma_labels):
    lookup = {}
    for label in rdma_labels:
        label_id = label["id"]
        if label_id is None:
            continue

        for name in (label["label_value"], label["display_name"]):
            if name:
                lookup[str(name)] = label_id

    return lookup


def resolve_rdma_network_selections(selections, list_labels_fn=None):
    """Resolve user-facing network label names to launch_workers selections."""
    if selections is None:
        return None

    if not isinstance(selections, list):
        raise RdmaNetworkSelectionError("RDMA network selections must be a list.")

    if len(selections) == 0:
        return []

    list_labels = list_labels_fn or list_node_labels_from_cmlapi
    rdma_labels = _available_rdma_network_labels(list_labels())
    lookup = _build_rdma_label_lookup(rdma_labels)

    resolved = []
    seen_names = set()

    for index, selection in enumerate(selections):
        if not isinstance(selection, dict):
            raise RdmaNetworkSelectionError(
                f"rdma_network_selections[{index}] must be a dict."
            )

        network_label = selection.get("network_label")
        if network_label is None:
            raise RdmaNetworkSelectionError(
                f"rdma_network_selections[{index}] must include 'network_label'."
            )

        network_label = str(network_label)
        quantity = selection.get("quantity")
        if quantity is None:
            raise RdmaNetworkSelectionError(
                f"rdma_network_selections[{index}].quantity must be at least 1."
            )

        try:
            quantity = int(quantity)
        except (TypeError, ValueError) as error:
            raise RdmaNetworkSelectionError(
                f"rdma_network_selections[{index}].quantity must be at least 1."
            ) from error

        if quantity < 1:
            raise RdmaNetworkSelectionError(
                f"rdma_network_selections[{index}].quantity must be at least 1."
            )

        if network_label in seen_names:
            raise RdmaNetworkSelectionError(
                "Duplicate RDMA network labels are not allowed."
            )
        seen_names.add(network_label)

        network_label_id = lookup.get(network_label)
        if network_label_id is None:
            raise RdmaNetworkSelectionError(
                f"No such RDMA network label: {network_label!r}."
            )

        resolved.append(
            {
                "network_label_id": int(network_label_id),
                "quantity": quantity,
            }
        )

    return resolved
