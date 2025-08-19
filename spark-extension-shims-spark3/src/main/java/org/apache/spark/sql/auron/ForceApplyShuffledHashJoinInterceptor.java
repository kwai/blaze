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
package org.apache.spark.sql.auron;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceApplyShuffledHashJoinInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(ForceApplyShuffledHashJoinInterceptor.class);

    @RuntimeType
    public static Object intercept(@Argument(0) Object conf) {
        logger.debug("calling JoinSelectionHelper.forceApplyShuffledHashJoin() intercepted by auron");
        return AuronConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf();
    }
}
