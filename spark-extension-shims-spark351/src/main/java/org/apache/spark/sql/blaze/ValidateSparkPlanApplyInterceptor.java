/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.blaze;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import org.apache.spark.sql.execution.SparkPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidateSparkPlanApplyInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(ValidateSparkPlanApplyInterceptor.class);

    @RuntimeType
    public static Object intercept(@Argument(0) Object plan) {
        logger.debug("calling ValidateSparkPlan.apply() intercepted by blaze");
        InterceptedValidateSparkPlan$.MODULE$.validate((SparkPlan) plan);
        return plan;
    }
}
