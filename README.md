# cmlextras

This python library has added functionality for [Cloudera Machine Learning (CML)](https://docs.cloudera.com/machine-learning/cloud/product/topics/ml-product-overview.html#cdsw_overview)'s cml (or legacy cdsw) library. The library is organized in modules and is built on the [CML Workers API](https://docs.cloudera.com/machine-learning/cloud/distributed-computing/topics/ml-workers-api.html) and other CML functionalities.

## Installation
This library can be installed directly from GitHub:
```%pip install git+https://github.com/cloudera/cmlextras.git```

## Modules

### Ray
Ray is a unified framework for scaling AI and Python applications. We can create a cluster on CML infrastructure to scale out Ray processes. This `cmlextras.ray_cluster` module abstracts the ray cluster provisioning and operations so users can focus on their application code instead of infrastructure management. 

Example usage:
```
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

### Workers_v2
The cml (or legacy cdsw) library has a workers module already. The v2 module is experimenting with a new management interface for the CML Workers infrastructure. The v2 module has more defaults and a more OOP approach for managing groups of workers. There is no added functionality, the v2 library relies on the functionality available in the orignal version. 

Example usage:
```
> import cmlextras.workers_v2 as workers
> from cmlextras.workers_v2 import WorkerGroup

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


## Contribute 
We welcome (and encourage!) all forms of contributions, including patches and PRs, reporting issues, submitting feature requests, and any feedback. 

