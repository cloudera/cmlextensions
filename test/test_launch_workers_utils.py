import sys
import os
import unittest
import warnings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from cmlextensions.launch_workers_utils import add_rdma_launch_args
from cmlextensions.rdma_network_utils import (
    RdmaNetworkSelectionError,
    resolve_rdma_network_selections,
)


def _launch_workers_without_rdma(n, cpu, memory, nvidia_gpu=0, code=""):
    pass


def _launch_workers_with_rdma(
    n,
    cpu,
    memory,
    nvidia_gpu=0,
    code="",
    rdma_network_selections=None,
):
    pass


def _sample_rdma_labels():
    return [
        {
            "id": 10,
            "label_key": "rdma.network.name",
            "label_value": "default/sriovib-network",
            "display_name": "SR-IOV IB Network",
            "availability": True,
        },
        {
            "id": 11,
            "label_key": "rdma.network.name",
            "label_value": "default/roce-network",
            "display_name": "RoCE Network",
            "availability": True,
        },
        {
            "id": 99,
            "label_key": "nvidia.com/gpu.product",
            "label_value": "A100",
            "display_name": "A100",
            "availability": True,
        },
        {
            "id": 12,
            "label_key": "rdma.network.name",
            "label_value": "default/removed-network",
            "display_name": "Removed Network",
            "availability": False,
        },
    ]


class TestResolveRdmaNetworkSelections(unittest.TestCase):
    def test_resolves_label_value_to_id(self):
        resolved = resolve_rdma_network_selections(
            [{"network_label": "default/sriovib-network", "quantity": 2}],
            list_labels_fn=lambda: _sample_rdma_labels(),
        )
        self.assertEqual(
            resolved,
            [{"network_label_id": 10, "quantity": 2}],
        )

    def test_resolves_display_name_to_id(self):
        resolved = resolve_rdma_network_selections(
            [{"network_label": "RoCE Network", "quantity": 1}],
            list_labels_fn=lambda: _sample_rdma_labels(),
        )
        self.assertEqual(
            resolved,
            [{"network_label_id": 11, "quantity": 1}],
        )

    def test_rejects_unknown_label(self):
        with self.assertRaisesRegex(
            RdmaNetworkSelectionError,
            "No such RDMA network label: 'missing-network'.",
        ):
            resolve_rdma_network_selections(
                [{"network_label": "missing-network", "quantity": 1}],
                list_labels_fn=lambda: _sample_rdma_labels(),
            )

    def test_rejects_duplicate_labels(self):
        with self.assertRaisesRegex(
            RdmaNetworkSelectionError,
            "Duplicate RDMA network labels are not allowed.",
        ):
            resolve_rdma_network_selections(
                [
                    {"network_label": "default/sriovib-network", "quantity": 1},
                    {"network_label": "default/sriovib-network", "quantity": 1},
                ],
                list_labels_fn=lambda: _sample_rdma_labels(),
            )

    def test_rejects_invalid_quantity(self):
        with self.assertRaisesRegex(
            RdmaNetworkSelectionError,
            "rdma_network_selections\\[0\\].quantity must be at least 1.",
        ):
            resolve_rdma_network_selections(
                [{"network_label": "default/sriovib-network", "quantity": 0}],
                list_labels_fn=lambda: _sample_rdma_labels(),
            )

    def test_requires_network_label_key(self):
        with self.assertRaisesRegex(
            RdmaNetworkSelectionError,
            "must include 'network_label'",
        ):
            resolve_rdma_network_selections(
                [{"quantity": 1}],
                list_labels_fn=lambda: _sample_rdma_labels(),
            )


class TestAddRdmaLaunchArgs(unittest.TestCase):
    def test_adds_resolved_rdma_network_selections_when_supported(self):
        args = {"n": 1, "cpu": 2, "memory": 4}
        add_rdma_launch_args(
            args,
            _launch_workers_with_rdma,
            rdma_network_selections=[
                {"network_label": "default/sriovib-network", "quantity": 2}
            ],
            list_labels_fn=lambda: _sample_rdma_labels(),
        )
        self.assertEqual(
            args["rdma_network_selections"],
            [{"network_label_id": 10, "quantity": 2}],
        )

    def test_skips_rdma_network_selections_when_unsupported(self):
        args = {"n": 1, "cpu": 2, "memory": 4}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            add_rdma_launch_args(
                args,
                _launch_workers_without_rdma,
                rdma_network_selections=[
                    {"network_label": "default/sriovib-network", "quantity": 2}
                ],
                list_labels_fn=lambda: _sample_rdma_labels(),
            )
        self.assertEqual(args, {"n": 1, "cpu": 2, "memory": 4})
        self.assertEqual(len(caught), 1)
        self.assertIs(caught[0].category, UserWarning)
        self.assertIn("rdma_network_selections", str(caught[0].message))
        self.assertIn("will be ignored", str(caught[0].message))

    def test_omits_none_rdma_network_selections(self):
        args = {"n": 1, "cpu": 2, "memory": 4}
        add_rdma_launch_args(args, _launch_workers_with_rdma)
        self.assertNotIn("rdma_network_selections", args)


if __name__ == "__main__":
    unittest.main()
