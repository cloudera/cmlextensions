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
import inspect

# PBJ Runtimes do not have the cdsw library installed.
# Instead, the cml library is added to workloads in recent CML releases.
try:
    import cml.utils_v1 as utils
    cdsw = utils._emulate_cdsw()
except ImportError:
    import cdsw


DEFAULT_DASHBOARD_PORT = os.environ['CDSW_APP_PORT']

class RayCluster():
    """Ray Cluster built on CML Worker infrastructure"""

    def __init__(self, num_workers, worker_cpu=2, worker_memory=4, worker_nvidia_gpu=0, head_cpu=2, head_memory=4, head_nvidia_gpu=0,  dashboard_port=DEFAULT_DASHBOARD_PORT, env
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
                    stop_responses = cdsw.stop_workers(*[workload["id"] for workload in workload_details[status]])
                    stop_response_statuses = [response.status_code for response in stop_responses]
                    if(any([status >= 300 for status in stop_response_statuses])):
                        print("Could not stop all Ray workloads. Trying to force stop all CML workers created in this session.")
                        cdsw.stop_workers()

    def _start_ray_workload(self, args, startup_timeout_seconds):
        workloads =  cdsw.launch_workers(**args)
        workloads_with_ids = [wl for wl in workloads if "id" in wl]
        workloads_with_no_ids = [wl for wl in workloads if "id" not in wl]
        no_id_messages = set([wl.get("message", "") for wl in workloads_with_no_ids])
        if len(workloads_with_no_ids) > 0:
            print("Could not create all requested workloads. Error messages received:" , no_id_messages)
            # Clean up all workloads
            ids = [wl["id"] for wl in workloads_with_ids ] + [wl.get("engineId", "") for wl in workloads_with_no_ids]
            ids = [id for id in ids if len(id) > 0]
            cdsw.stop_workers(*ids)
            return None
        workload_details =  cdsw.await_workers(
          workloads_with_ids,
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
        if "name" in inspect.signature(cdsw.launch_workers).parameters:
            args['name'] = 'Ray Head'

        self.ray_head_details = self._start_ray_workload(args, startup_timeout_seconds)

    def _add_ray_workers(self, head_addr, startup_timeout_seconds):
        # We need to start the ray process with --block else the command completes and the CML Worker terminates
        worker_start_cmd = f"!ray start --block --num-cpus={self.worker_cpu} --address={head_addr}"

        args = {
            'n': self.num_workers,
            'cpu': self.worker_cpu,
            'memory': self.worker_memory,
            "nvidia_gpu" : self.worker_nvidia_gpu,
            'code': worker_start_cmd,
            'env': self.env,
        }

        if "name" in inspect.signature(cdsw.launch_workers).parameters:
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
        print("Starting ray head...")
        startup_failed = False
        self._start_ray_head(startup_timeout_seconds = startup_timeout_seconds)

        if len(self.ray_head_details.get("workers",[])) < 1:
            print(f"Could not start ray head.")
            startup_failed = True

        else:
          ray_head_ip = self.ray_head_details['workers'][0]['ip_address']
          ray_head_addr = ray_head_ip + ':6379'
          print(f"Starting {self.num_workers} ray workers...")
          self._add_ray_workers(ray_head_addr, startup_timeout_seconds = startup_timeout_seconds)
          if self.ray_worker_details is None or len(self.ray_worker_details.get("workers", [])) < self.num_workers:
              print(f"Could not start {self.num_workers} requested ray workers.\n")
              startup_failed = True

        if startup_failed:
          print("Could not start some of the ray workloads. Ensure ray is able to run in your environment and you have the resources in your CML workspace to provision the specified amount of ray workloads.")
          print("Set a longer timeout period if your CML workspace needs time to scale.")
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
