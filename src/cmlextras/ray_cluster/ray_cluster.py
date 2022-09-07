import os
import cdsw

DEFAULT_DASHBOARD_PORT = os.environ['CDSW_APP_PORT']

class RayCluster():
    """Ray Cluster built on CML Worker infrastructure"""

    def __init__(self, num_workers, worker_cpu=2, worker_memory=4, head_cpu=2, head_memory=4, dashboard_port=DEFAULT_DASHBOARD_PORT):
        self.num_workers = num_workers
        self.worker_cpu = worker_cpu
        self.worker_memory = worker_memory
        self.head_cpu = head_cpu
        self.head_memory = head_memory
        self.dashboard_port = dashboard_port
        
        self.ray_head_details = None
        self.ray_worker_details = None


    def _start_ray_head(self):
        # We need to start the ray process with --block else the command completes and the CML Worker terminates
        head_start_cmd = f"!ray start --head --block --include-dashboard=true --dashboard-port={self.dashboard_port}"
        ray_head = cdsw.launch_workers(
            n=1,
            cpu=self.head_cpu,
            memory=self.head_memory,
            code=head_start_cmd,
        )

        self.ray_head_details = cdsw.await_workers(
          ray_head, 
          wait_for_completion=False, 
          timeout_seconds=90
        )

    def _add_ray_workers(self, head_addr):
        # We need to start the ray process with --block else the command completes and the CML Worker terminates
        worker_start_cmd = f"!ray start --block --address={head_addr}"
        ray_workers = cdsw.launch_workers(
            n=self.num_workers, 
            cpu=self.worker_cpu, 
            memory=self.worker_memory, 
            code=worker_start_cmd,
        )

        self.ray_worker_details = cdsw.await_workers(
            ray_workers, 
            wait_for_completion=False)

    def init(self):
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
        self._start_ray_head()

        ray_head_ip = self.ray_head_details['workers'][0]['ip_address']
        ray_head_addr = ray_head_ip + ':6379'
        
        self._add_ray_workers(ray_head_addr)
        
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
        
        #TODO: stop workers only when they were created for this Ray Cluster
        cdsw.stop_workers()
        
        # Reset instance state
        self.ray_head_ip = None
        self.ray_head_addr = None
        self.ray_head_details = None
        self.ray_worker_details = None
