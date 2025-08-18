/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.auron

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hive.{HiveGenericUDF, HiveSimpleUDF}
import org.apache.spark.sql.hive.HiveUDAFFunction

object HiveUDFUtil extends Logging {
  def isHiveUDF(expression: Expression): Boolean = {
    isHiveSimpleUDF(expression) || isHiveGenericUDF(expression)
  }

  def isHiveSimpleUDF(expression: Expression): Boolean = {
    expression.isInstanceOf[HiveSimpleUDF]
  }

  def isHiveGenericUDF(expression: Expression): Boolean = {
    expression.isInstanceOf[HiveGenericUDF]
  }

  def getFunctionClassName(expression: Expression): Option[String] = {
    expression match {
      case e: HiveSimpleUDF => Some(e.funcWrapper.functionClassName)
      case e: HiveGenericUDF => Some(e.funcWrapper.functionClassName)
      case e: HiveUDAFFunction => Some(e.funcWrapper.functionClassName)
      case _ => None
    }
  }
}
