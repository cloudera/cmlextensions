# cmlextensions

This python library has added functionality for [Cloudera Machine Learning (CML)](https://docs.cloudera.com/machine-learning/cloud/product/topics/ml-product-overview.html#cdsw_overview)'s cml (or legacy cdsw) library. The library is organized in modules and is built on the [CML Workers API](https://docs.cloudera.com/machine-learning/cloud/distributed-computing/topics/ml-workers-api.html) and other CML functionalities.

## Installation
This library can be installed directly from GitHub:

```%pip install git+https://github.com/cloudera/cmlextensions.git```

## Modules

### Ray
Ray is a unified framework for scaling AI and Python applications. We can create a cluster on CML infrastructure to scale out Ray processes. This `cmlextensions.ray_cluster` module abstracts the ray cluster provisioning and operations so users can focus on their application code instead of infrastructure management.

Example usage:
```
> from cmlextensions.ray_cluster import RayCluster

> cluster = RayCluster(num_workers=2)
> cluster.init()

--------------------
Ray cluster started
--------------------

The Ray dashboard is running at
https://024d0wpuw0eain8r.ml-4c5feac0-3ec.go01-dem.ylcu-atmi.cloudera.site/

To connect to this Ray cluster from this CML Session,
use the following Python code:
  import ray
  ray.init(address='ray://100.100.127.74:10001')

```

### Dask
Dask is a flexible parallel computing library for analytics in Python. We can create a cluster on CML infrastructure to scale out Dask processes. This `cmlextensions.dask_cluster` module abstracts the dask cluster provisioning and operations so users can focus on their application code instead of infrastructure management.

Example usage:
```
> from cmlextensions.dask_cluster import DaskCluster

> cluster = DaskCluster(num_workers=2)
> cluster.init()

--------------------
Dask cluster started
--------------------

The Dask dashboard is running at
https://024d0wpuw0eain8r.ml-4c5feac0-3ec.go01-dem.ylcu-atmi.cloudera.site/

To connect to this Dask cluster from this CML Session,
use the following Python code:
  from dask.distributed import Client
  client = Client('tcp://100.100.225.149:8786')
```

### Dask Cuda
Dask CUDA is a library that integrates Dask with NVIDIA CUDA to enable scalable, distributed computing on GPUs. It provides tools for managing GPU resources, scheduling GPU-aware tasks, and efficiently moving data between CPUs and GPUs across single machines or clusters. Dask CUDA is commonly used with RAPIDS libraries to accelerate data processing, machine learning, and analytics workflows on NVIDIA GPUs.

Example usage:
```
> from cmlextensions.dask_cluster import DaskCluster

> cluster = DaskCluster(num_workers=2, worker_cpu=4, nvidia_gpu=2, worker_memory=12, scheduler_cpu=4, scheduler_memory=12)
> cluster.init()

--------------------
Dask cluster started
--------------------

The Dask dashboard is running at
https://024d0wpuw0eain8r.ml-4c5feac0-3ec.go01-dem.ylcu-atmi.cloudera.site/

To connect to this Dask cluster from this CML Session,
use the following Python code:
  from dask.distributed import Client
  client = Client('tcp://100.100.225.149:8786')
```

### RDMA network support
When your CML workspace has RDMA network labels configured, you can request RDMA resources when launching workers. Specify RDMA networks by label name (the `label_value` stored for `rdma.network.name` in the node labels table). cmlextensions resolves names to internal IDs via `cmlapi` before calling `launch_workers()`. On older runtimes that do not support RDMA, these parameters are omitted automatically.

Example usage with Ray:
```
> from cmlextensions.ray_cluster import RayCluster

> cluster = RayCluster(
...     num_workers=4,
...     worker_cpu=4,
...     worker_memory=16,
...     worker_nvidia_gpu=2,
...     worker_rdma_network_selections=[{"network_label": "default/sriovib-network", "quantity": 2}],
...     head_rdma_network_selections=[{"network_label": "default/sriovib-network", "quantity": 1}],
... )
> cluster.init()
```

Example usage with Dask:
```
> from cmlextensions.dask_cluster import DaskCluster

> cluster = DaskCluster(
...     num_workers=4,
...     worker_cpu=4,
...     worker_memory=16,
...     nvidia_gpu=2,
...     rdma_network_selections=[{"network_label": "default/roce-network", "quantity": 2}],
... )
> cluster.init()
```

Example usage with WorkerGroup:
```
> from cmlextensions.workers_v2 import WorkerGroup

> wg = WorkerGroup(
...     n=4,
...     cpu=4,
...     memory=16,
...     nvidia_gpu=2,
...     rdma_network_selections=[{"network_label": "default/sriovib-network", "quantity": 2}],
...     code="print('Hello from a worker with RDMA')",
... )
```

### Workers_v2
The cml (or legacy cdsw) library has a workers module already. The v2 module is experimenting with a new management interface for the CML Workers infrastructure. The v2 module has more defaults and a more OOP approach for managing groups of workers. There is no added functionality, the v2 library relies on the functionality available in the orignal version.

Example usage:
```
> import cmlextensions.workers_v2 as workers
> from cmlextensions.workers_v2 import WorkerGroup

> wg1 = WorkerGroup(1, code="import time;time.sleep(300)")
> wg1.get_workers()
id	status	created_at	running_at	finished_at	duration	ip_address
221pa78rmzau93zf	running	2022-09-09T12:02:14.031Z	2022-09-09T12:02:27.945Z	None	1	100.100.209.35

> workers.get_workers(active=True)
id	status	created_at	running_at	finished_at	duration	ip_address
221pa78rmzau93zf	running	2022-09-09T12:02:14.031Z	2022-09-09T12:02:27.945Z	None	7	100.100.209.35
6tyvg0kuu0wrlcyl	running	2022-09-09T12:01:50.282Z	2022-09-09T12:02:04.387Z	None	30	100.100.127.80

> wg1.stop_workers()
```
