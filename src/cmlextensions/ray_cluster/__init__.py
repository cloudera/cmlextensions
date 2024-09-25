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

from .ray_cluster import *

try:
    import cml.utils_v1 as utils

    cdsw = utils._emulate_cdsw()
except ImportError:
    import cdsw

if "cdsw" not in locals():
    raise ImportError(
        "Could not import cdsw, for this module to work you need to execute this code in a CML session"
    )
