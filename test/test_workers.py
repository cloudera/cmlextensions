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

from cmlextras.workers_v2 import WorkerGroup
import cmlextras.workers_v2 as workers
import time
import sys
sys.path.append('../src')

# check no workers
workers.get_workers()

# test get and describe workers
wg = WorkerGroup(2, code="import time;time.sleep(30)")
wg.get_workers()
wg.describe_workers()


# test stop workers
wg2 = WorkerGroup(1, code="import time;time.sleep(300)")
time.sleep(10)
wg2.get_workers()
wg2.stop_workers()

# test active workload filter
workers.get_workers()
wg4 = WorkerGroup(1, code="import time;time.sleep(300)")
wg4.get_workers()
workers.get_workers(active=True)

# test awaits
start = time.time()
wg4 = WorkerGroup(1, wait_for_running=True, code="import time;time.sleep(1)")
end = time.time()
print("Time waited for getting to running status: ", end - start)

start2 = time.time()
wg5 = WorkerGroup(1, wait_for_running=True,
                  wait_for_completion=True, code="import time;time.sleep(30)")
end2 = time.time()
print("Time waited for completion: ", end2 - start2)

workers.stop_workers()
