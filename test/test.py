import sys
sys.path.append('../src')

from cmlextras.ray_cluster import RayCluster

c = RayCluster(num_workers=2)
c.init()

# Connect to the cluster
import ray
ray.init(address=c.get_client_url())

# Define the square task.
@ray.remote
def square(x):
    return x * x

# Launch four parallel square tasks.
futures = [square.remote(i) for i in range(4)]

# Retrieve results.
print(ray.get(futures))

# Delete cluster
c.terminate()