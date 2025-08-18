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
package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.auron.sparkver

case object NativeProjectExecProvider {
  @sparkver("3.4 / 3.5")
  def provide(projectList: Seq[NamedExpression], child: SparkPlan): NativeProjectBase = {
    import org.apache.spark.sql.execution.OrderPreservingUnaryExecNode
    import org.apache.spark.sql.execution.PartitioningPreservingUnaryExecNode

    case class NativeProjectExec(projectList: Seq[NamedExpression], override val child: SparkPlan)
        extends NativeProjectBase(projectList, child)
        with PartitioningPreservingUnaryExecNode
        with OrderPreservingUnaryExecNode {

      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)

      override protected def outputExpressions = projectList

      override protected def orderingExpressions = child.outputOrdering

      override def nodeName: String = "NativeProjectExec"
    }
    NativeProjectExec(projectList, child)
  }

  @sparkver("3.1 / 3.2 / 3.3")
  def provide(projectList: Seq[NamedExpression], child: SparkPlan): NativeProjectBase = {
    import org.apache.spark.sql.execution.AliasAwareOutputOrdering
    import org.apache.spark.sql.execution.AliasAwareOutputPartitioning

    case class NativeProjectExec(projectList: Seq[NamedExpression], override val child: SparkPlan)
        extends NativeProjectBase(projectList, child)
        with AliasAwareOutputPartitioning
        with AliasAwareOutputOrdering {

      @sparkver("3.2 / 3.3")
      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)

      @sparkver("3.1")
      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(child = newChildren.head)

      override protected def outputExpressions = projectList

      override protected def orderingExpressions = child.outputOrdering

      override def nodeName: String = "NativeProjectExec"
    }
    NativeProjectExec(projectList, child)
  }

  @sparkver("3.0")
  def provide(projectList: Seq[NamedExpression], child: SparkPlan): NativeProjectBase = {

    case class NativeProjectExec(projectList: Seq[NamedExpression], override val child: SparkPlan)
        extends NativeProjectBase(projectList, child) {

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(child = newChildren.head)

      override def nodeName: String = "NativeProjectExec"
    }
    NativeProjectExec(projectList, child)
  }
}
