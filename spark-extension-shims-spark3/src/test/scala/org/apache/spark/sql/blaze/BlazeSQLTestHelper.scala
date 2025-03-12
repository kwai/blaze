package org.apache.spark.sql.blaze

import org.apache.spark.SparkEnv

trait BlazeSQLTestHelper {
  def withEnvConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val env = SparkEnv.get
    if (env == null) {
      throw new IllegalStateException("SparkEnv is not initialized")
    }
    val conf = env.conf
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.get(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      conf.set(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.remove(key)
      }
    }
  }
}
