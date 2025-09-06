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
package org.apache.spark.sql.auron.util

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.util.Utils

object TaskContextHelper extends Logging {

  private val callerContextSupported: Boolean = {
    SparkHadoopUtil.get.conf.getBoolean("hadoop.caller.context.enabled", false) && {
      try {
        Utils.classForName("org.apache.hadoop.ipc.CallerContext")
        Utils.classForName("org.apache.hadoop.ipc.CallerContext$Builder")
        true
      } catch {
        case _: ClassNotFoundException =>
          false
        case NonFatal(e) =>
          logWarning("Fail to load the CallerContext class", e)
          false
      }
    }
  }

  def setNativeThreadName(): Unit = {
    val context: TaskContext = TaskContext.get()
    val thread = Thread.currentThread()
    val threadName = if (context != null) {
      s"auron native task ${context.partitionId()}.${context.attemptNumber()} in stage ${context
        .stageId()}.${context.stageAttemptNumber()} (TID ${context.taskAttemptId()})"
    } else {
      "auron native task " + thread.getName
    }
    thread.setName(threadName)
  }

  def setHDFSCallerContext(): Unit = {
    if (!callerContextSupported) {
      return
    }
    val context: TaskContext = TaskContext.get()
    if (context != null) {
      val conf = SparkEnv.get.conf
      val appId = conf.get("spark.app.id", "")
      val appAttemptId = conf.get("spark.app.attempt.id", "")
      // Spark executor cannot get the jobId from TaskContext, so we set a default value -1 here.
      val jobId = -1
      new CallerContextHelper(
        "TASK",
        conf.get(APP_CALLER_CONTEXT),
        Option(appId),
        if (appAttemptId == "") None else Option(appAttemptId),
        Option(jobId),
        Option(context.stageId()),
        Option(context.stageAttemptNumber()),
        Option(context.taskAttemptId()),
        Option(context.attemptNumber())).setCurrentContext()
    }
  }

  /**
   * Copied from Apache Spark org.apache.spark.util.CallerContext
   */
  private class CallerContextHelper(
      from: String,
      upstreamCallerContext: Option[String] = None,
      appId: Option[String] = None,
      appAttemptId: Option[String] = None,
      jobId: Option[Int] = None,
      stageId: Option[Int] = None,
      stageAttemptId: Option[Int] = None,
      taskId: Option[Long] = None,
      taskAttemptNumber: Option[Int] = None)
      extends Logging {

    private val context = prepareContext(
      "SPARK_" +
        from +
        appId.map("_" + _).getOrElse("") +
        appAttemptId.map("_" + _).getOrElse("") +
        jobId.map("_JId_" + _).getOrElse("") +
        stageId.map("_SId_" + _).getOrElse("") +
        stageAttemptId.map("_" + _).getOrElse("") +
        taskId.map("_TId_" + _).getOrElse("") +
        taskAttemptNumber.map("_" + _).getOrElse("") +
        upstreamCallerContext.map("_" + _).getOrElse(""))

    private def prepareContext(context: String): String = {
      lazy val len = SparkHadoopUtil.get.conf.getInt("hadoop.caller.context.max.size", 128)
      if (context == null || context.length <= len) {
        context
      } else {
        val finalContext = context.substring(0, len)
        logWarning(s"Truncated Spark caller context from $context to $finalContext")
        finalContext
      }
    }

    def setCurrentContext(): Unit = {
      if (callerContextSupported) {
        try {
          val callerContext = Utils.classForName("org.apache.hadoop.ipc.CallerContext")
          val builder: Class[AnyRef] =
            Utils.classForName("org.apache.hadoop.ipc.CallerContext$Builder")
          val builderInst = builder.getConstructor(classOf[String]).newInstance(context)
          val hdfsContext = builder.getMethod("build").invoke(builderInst)
          callerContext.getMethod("setCurrent", callerContext).invoke(null, hdfsContext)
        } catch {
          case NonFatal(e) =>
            logWarning("Fail to set Spark caller context", e)
        }
      }
    }
  }
}
