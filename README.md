# cml-extras

This python library has added functionality for [Cloudera Machine Learning (CML)](https://docs.cloudera.com/machine-learning/cloud/product/topics/ml-product-overview.html#cdsw_overview)'s cml library. The library is organized in modules and is built on the [CML Workers API](https://docs.cloudera.com/machine-learning/cloud/distributed-computing/topics/ml-workers-api.html) and other CML functionalities.

## Installation
This library can be installed directly from GitHub:
```%pip install git+https://github.com/peterableda/cml-extras.git```

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

## Contribute 
We welcome (and encourage!) all forms of contributions, including patches and PRs, reporting issues, submitting feature requests, and any feedback. 

