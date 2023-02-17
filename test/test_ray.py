# Copyright 2022 Cloudera. All Rights Reserved.
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

# Add cmlextensions to the path
import sys
sys.path.append('../src')

import ray
from cmlextensions.ray_cluster import RayCluster

c = RayCluster(num_workers=2, env={'OMP_NUM_THREADS':'2'})
c.init()

# Connect to the cluster
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
