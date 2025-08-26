#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Function to display script usage
print_help() {
    echo "Usage: $0 [OPTIONS] <maven build options>"
    echo "Build Auron project with specified Maven profiles"
    echo
    echo "Options:"
    echo "  --pre                  Activate pre-release profile"
    echo "  --release              Activate release profile"
    echo "  --sparkver <VERSION>   Specify Spark version (e.g. 3.0/3.1/3.2/3.3/3.4/3.5)"
    echo "  --scalaver <VERSION>   Specify Scala version (e.g. 2.12/2.13)"
    echo "  --celeborn <VERSION>   Specify Celeborn version (e.g. 0.5/0.6)"
    echo "  --uniffle <VERSION>    Specify Uniffle version (e.g. 0.9)"
    echo "  --paimon <VERSION>     Specify Paimon version (e.g. 1.1)"
    echo "  --clean <true/false>   Clean before build, default: true"
    echo "  -h, --help             Show this help message"
    echo
    echo "Profile mapping:"
    echo "  --pre           → -Ppre"
    echo "  --release       → -Prelease"
    echo "  --sparkver 3.5  → -Pspark-3.5"
    echo "  --celeborn 0.5  → -Pceleborn,celeborn-0.5"
    echo "  --uniffle 0.9   → -Puniffle,uniffle-0.9"
    echo "  --paimon 1.1    → -Ppaimon,paimon-1.1"
    echo
    echo "Examples:"
    echo "  $0 --pre --sparkver 3.5 --scalaver 2.13"
    echo "  $0 --release --sparkver 3.5 --scalaver 2.13 --celeborn 0.5 --uniffle 0.9 --paimon 1.1"
    exit 0
}

MVN_CMD="$(dirname "$0")/build/mvn"

# Initialize variables
PRE_PROFILE=false
RELEASE_PROFILE=false
CLEAN=true
SPARK_VER=""
SCALA_VER=""
CELEBORN_VER=""
UNIFFLE_VER=""
PAIMON_VER=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pre)
            PRE_PROFILE=true
            shift
            ;;
        --release)
            RELEASE_PROFILE=true
            shift
            ;;
        --clean)
            if [[ -n "$2" && "$2" =~ ^(true|false)$ ]]; then
                CLEAN="$2"
                shift 2
            else
                echo "ERROR: --clean requires true/false" >&2
                exit 1
            fi
            ;;
        --sparkver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SPARK_VER="$2"
                shift 2
            else
                echo "ERROR: --sparkver requires version argument" >&2
                exit 1
            fi
            ;;
        --scalaver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SCALA_VER="$2"
                shift 2
            else
                echo "ERROR: --scalaver requires version argument" >&2
                exit 1
            fi
            ;;
        --celeborn)
            if [[ -n "$2" && "$2" != -* ]]; then
                CELEBORN_VER="$2"
                shift 2
            else
                echo "ERROR: --celeborn requires version argument" >&2
                exit 1
            fi
            ;;
        --uniffle)
            if [[ -n "$2" && "$2" != -* ]]; then
                UNIFFLE_VER="$2"
                shift 2
            else
                echo "ERROR: --uniffle requires version argument" >&2
                exit 1
            fi
            ;;
        --paimon)
            if [[ -n "$2" && "$2" != -* ]]; then
                PAIMON_VER="$2"
                shift 2
            else
                echo "ERROR: --paimon requires version argument" >&2
                exit 1
            fi
            ;;
        -h|--help)
            print_help
            ;;
        --*)
            echo "ERROR: Unknown option '$1'" >&2
            echo "Use '$0 --help' for usage information" >&2
            exit 1
            ;;
        -*)
            break
            ;;
        *)
            echo "ERROR: $1 is not supported" >&2
            echo "Use '$0 --help' for usage information" >&2
            exit 1
            ;;
    esac
done

# Validate requirements
if [[ "$PRE_PROFILE" == false && "$RELEASE_PROFILE" == false ]]; then
    MISSING_REQUIREMENTS+=("--pre or --release must be specified")
fi

if [[ -z "$SPARK_VER" ]]; then
    MISSING_REQUIREMENTS+=("--sparkver must be specified")
fi

if [[ -z "$SCALA_VER" ]]; then
    MISSING_REQUIREMENTS+=("--scalaver must be specified")
fi

if [[ "${#MISSING_REQUIREMENTS[@]}" -gt 0 ]]; then
    echo "ERROR: Missing required arguments:" >&2
    for req in "${MISSING_REQUIREMENTS[@]}"; do
        echo "  * $req" >&2
    done
    echo
    echo "Use '$0 --help' for usage information" >&2
    exit 1
fi

# Validate profile combinations
if [[ "$PRE_PROFILE" == true && "$RELEASE_PROFILE" == true ]]; then
    echo "ERROR: Cannot use both --pre and --release simultaneously" >&2
    exit 1
fi

# Build Maven args
CLEAN_ARGS=()
BUILD_ARGS=(package -DskipTests)

# Add profile flags
if [[ "$CLEAN" == true ]]; then
    CLEAN_ARGS=(clean)
fi
if [[ "$PRE_PROFILE" == true ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Ppre")
fi
if [[ "$RELEASE_PROFILE" == true ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Prelease")
fi
if [[ -n "$SPARK_VER" ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Pspark-$SPARK_VER")
fi
if [[ -n "$SCALA_VER" ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Pscala-$SCALA_VER")
fi
if [[ -n "$CELEBORN_VER" ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Pceleborn,celeborn-$CELEBORN_VER")
fi
if [[ -n "$UNIFFLE_VER" ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Puniffle,uniffle-$UNIFFLE_VER")
fi
if [[ -n "$PAIMON_VER" ]]; then
    BUILD_ARGS=("${BUILD_ARGS[@]}" "-Ppaimon,paimon-$PAIMON_VER")
fi
MVN_ARGS=("${CLEAN_ARGS[@]}" "${BUILD_ARGS[@]}")

# Execute Maven command
echo "Compiling with maven args: $MVN_CMD ${MVN_ARGS[@]} $@"
"$MVN_CMD" "${MVN_ARGS[@]}" "$@"
