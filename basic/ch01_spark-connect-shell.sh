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

# The shell script to start a spark-shell with spark connect enabled.

if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

# /Users/jeongmo/.sdkman/candidates/spark/current/bin/spark-connect-shell
# Set SPARK_LOCAL_IP// java.net.BindException: Can't assign requested address: Service 'sparkDriver'  에러 처리
export SPARK_LOCAL_IP="127.0.0.1"  

# This requires building the spark with `-Pconnect`, e,g, `build/sbt -Pconnect package`
exec "${SPARK_HOME}"/bin/spark-shell --conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin "$@"