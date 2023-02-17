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

## Dependencies
# !pip install dask[complete]

# Add cmlextensions to the path
import sys
sys.path.append('../src')

from cmlextensions.dask_cluster import DaskCluster

cluster = DaskCluster(num_workers=2)
cluster.init()

# Connect to the cluster
from dask.distributed import Client
client = Client(cluster.get_client_url())

client

import dask.array as da

# Create a dask array from a NumPy array
x = da.from_array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], chunks=(2, 2))

# Perform a computation on the dask array
y = (x + 1) * 2

# Submit the computation to the cluster for execution
future = client.submit(y.compute)

# Wait for the computation to complete and retrieve the result
result = future.result()

print(result)  # Outputs: [[ 4  6  8] [10 12 14] [14 16 18]]

# Delete cluster
cluster.terminate()
