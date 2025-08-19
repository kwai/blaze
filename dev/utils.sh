#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Common functions in scripts

# Function to join an input array by a given separator
join_by() {
  local IFS="$1"
  shift
  echo "$*"
}

# Function to run a given command
run_cmd() {
  local command="$1"
  local working_dir="$2"

  # Preserve the calling directory
  _CALLING_DIR="$(pwd)"

  # Run the given command and check if it works well
  cd ${working_dir} && ${command}
  if [ $? = 127 ]; then
    echo "Cannot run '${command}', so check if the command works"
    exit 1
  fi

  # Reset the current working directory
  cd "${_CALLING_DIR}"
}

# Split input arguments into two parts: Spark confs and args
parse_args_for_spark_submit() {
  SPARK_CONF=()
  ARGS=()
  while [ ! -z "$1" ]; do
    if [[ "$1" =~ ^--master= ]]; then
      SPARK_CONF+=($1)
    elif [ "$1" == "--conf" ]; then
      shift
      SPARK_CONF+=("--conf $1")
    else
      ARGS+=($1)
    fi
    shift
  done
}

