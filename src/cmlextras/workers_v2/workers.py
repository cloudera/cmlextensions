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

import cdsw
import pandas as pd
import uuid

pd.set_option('display.max_columns', None)

def get_workers(active=False):
    if active:
        workers = _get_active_workers()
    else:
        workers = cdsw.list_workers()

    # TODO: it would be nice to return the worker group_ids
    return _worker_dict_to_df(workers)

def describe_workers(active=False):
    if active:
        return _get_active_workers()
    else:
        return cdsw.list_workers()

def stop_workers():
    return cdsw.stop_workers()

def _worker_dict_to_df(worker_dict):
    df = pd.DataFrame.from_dict(worker_dict)

    # TODO once we can set the worker 'name' we should show it
    columns = ['id', 'status', 'created_at', 'running_at', 'finished_at', 'duration', 'ip_address']

    # constructing df in case of missing columns
    for col in columns:
        if col not in df:
            df[col] = None

    return df[columns]

def _get_active_workers():
    workers = cdsw.list_workers()

    stopped_statuses = ['failed', 'succeeded', 'stopped']

    return [
            worker for worker in workers if worker["status"] not in stopped_statuses
        ]

class WorkerGroup():
    """New interface for the CML Worker infrastructure"""

    def __init__(self, n, cpu=2, memory=4, nvidia_gpu=0, script="", code="", env={}, wait_for_running=False, wait_for_completion=False, timeout_seconds=90):

        self.id = str(uuid.uuid4())[:8]
        # env['group_id'] = self.id

        workers = cdsw.launch_workers(
            n=n,
            cpu=cpu,
            memory=memory,
            nvidia_gpu=nvidia_gpu,
            script=script,
            code=code,
            env=env,
        )

        self.worker_ids = [
            worker["id"] for worker in workers
        ]

        if wait_for_running or wait_for_completion:
            self.failures = cdsw.await_workers(
                workers,
                wait_for_completion=wait_for_completion,
                timeout_seconds=timeout_seconds
            )['failures']


    def _get_fresh_worker_data(self):
        refreshed_workers = cdsw.list_workers()

        return [
            worker for worker in refreshed_workers if worker["id"] in self.worker_ids
        ]

    def describe_workers(self):
        return self._get_fresh_worker_data()

    def get_workers(self):
        return _worker_dict_to_df(self._get_fresh_worker_data())

    def stop_workers(self):
        return cdsw.stop_workers(*self.worker_ids)
