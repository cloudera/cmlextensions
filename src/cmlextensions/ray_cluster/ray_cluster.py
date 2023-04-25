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

import os
import cdsw

DEFAULT_DASHBOARD_PORT = os.environ['CDSW_APP_PORT']

class RayCluster():
    """Ray Cluster built on CML Worker infrastructure"""

    def __init__(self, num_workers, worker_cpu=2, worker_memory=4, worker_nvidia_gpu  = 0, head_cpu=2, head_memory=4, head_nvidia_gpu = 0,  dashboard_port=DEFAULT_DASHBOARD_PORT, env
={}):
        self.num_workers = num_workers
        self.worker_cpu = worker_cpu
        self.worker_memory = worker_memory
        self.worker_nvidia_gpu = worker_nvidia_gpu
        self.head_cpu = head_cpu
        self.head_memory = head_memory
        self.head_nvidia_gpu = head_nvidia_gpu
        self.dashboard_port = dashboard_port
        self.env = env

        self.ray_head_details = None
        self.ray_worker_details = None

    def _stop_ray_workloads(self):
        for workload_details in [self.ray_head_details, self.ray_worker_details]:
            for status in ["workers" , "failures"]:
                if workload_details is not None and status in workload_details:
                    cdsw.stop_workers(workload_details[status])

    def _start_ray_workload(self, args, startup_timeout_seconds):
        workloads =  cdsw.launch_workers(**args)
        workload_details =  cdsw.await_workers(
          workloads,
          wait_for_completion=False,
          timeout_seconds=startup_timeout_seconds
        )
        return workload_details
                
        


    def _start_ray_head(self, startup_timeout_seconds):
        # We need to start the ray process with --block else the command completes and the CML Worker terminates
        head_start_cmd = f"!ray start --head --block --disable-usage-stats --num-cpus={self.head_cpu} --include-dashboard=true --dashboard-port={self.dashboard_port}"

        args = {
            'n': 1,
            'cpu': self.head_cpu,
            'memory': self.head_memory,
            "nvidia_gpu" : self.head_nvidia_gpu,
            'code': head_start_cmd,
            'env': self.env,
        }

        if hasattr(cdsw.launch_workers, 'name'):
            args['name'] = 'Ray Head'

        self.ray_head_details = self._start_ray_workload(args, startup_timeout_seconds)

    def _add_ray_workers(self, head_addr, startup_timeout_seconds):
        # We need to start the ray process with --block else the command completes and the CML Worker terminates
        worker_start_cmd = f"!ray start --block --num-cpus={self.worker_cpu} --address={head_addr}"

        args = {
            'n': self.num_workers,
            'cpu': self.worker_cpu,
            'memory': self.worker_memory,
            'code': worker_start_cmd,
            'env': self.env,
        }

        if hasattr(cdsw.launch_workers, 'name'):
            args['name'] = 'Ray Worker'

        self.ray_worker_details = self._start_ray_workload(args, startup_timeout_seconds)

    def init(self, startup_timeout_seconds = 90):
        """
        Creates a Ray Cluster on the CML Workers infrastructure.
        """
        try:
            import ray  # pylint: disable=unused-import
        except ImportError as error:
            raise ImportError(
                "Could not import ray, for this module to work please run `pip install ray[default]` \n -> "
                + str(error)
            ) from error

        # Start the ray head process
        startup_failed = False
        self._start_ray_head(startup_timeout_seconds = startup_timeout_seconds)

        if "failures" in self.ray_head_details and len(self.ray_head_details["failures"]) > 0:
            print(f"Could not start up ray head.")
            startup_failed = True

        else:
          ray_head_ip = self.ray_head_details['workers'][0]['ip_address']
          ray_head_addr = ray_head_ip + ':6379'
          self._add_ray_workers(ray_head_addr, startup_timeout_seconds = startup_timeout_seconds)
          if "failures" in self.ray_worker_details and  len(self.ray_worker_details["failures"]) > 0 :
              print(f"Could not start up {len(self.ray_worker_details["failures"])} ray workers.")
          startup_failed = True

        if startup_failed:
          print("Cold not start some of they ray workloads. Ensure you have the resources in your CML cluster to provision the specified ray workloads and try again.")
          print("Set a longer timeout period if your cluster needs time to scale.")
          print("Shutting down Ray cluster..")
          self.terminate()
          return

        #TODO: could add cluster details, e.g., worker count and resources
        print(f"""
--------------------
Ray cluster started
--------------------

The Ray dashboard is running at
{self.get_dashboard_url()}

To connect to this Ray cluster from this CML Session,
use the following Python code:
  import ray
  ray.init(address='{self.get_client_url()}')
""")

    def get_dashboard_url(self):
        """
        Return the Ray dashboard url.
        """
        try:
            return self.ray_head_details['workers'][0]['app_url']
        except Error as error:
            raise Error("ERROR: Ray cluster is not running!")

    def get_client_url(self):
        ray_head_ip = self.ray_head_details['workers'][0]['ip_address']
        return f"ray://{ray_head_ip}:10001"

    def terminate(self):
        """
        Terminates the Ray Cluster.
        """

        self._stop_ray_workloads()

        # Reset instance state
        self.ray_head_ip = None
        self.ray_head_addr = None
        self.ray_head_details = None
        self.ray_worker_details = None
