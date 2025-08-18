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

import static net.bytebuddy.matcher.ElementMatchers.named;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.pool.TypePool;

public class ForceApplyShuffledHashJoinInjector {
    public static void inject() {
        ByteBuddyAgent.install();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        TypeDescription typeDescription = TypePool.Default.of(contextClassLoader)
                .describe("org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper")
                .resolve();
        new ByteBuddy()
                .redefine(typeDescription, ClassFileLocator.ForClassLoader.of(contextClassLoader))
                .method(named("forceApplyShuffledHashJoin"))
                .intercept(MethodDelegation.to(ForceApplyShuffledHashJoinInterceptor.class))
                .make()
                .load(contextClassLoader, ClassLoadingStrategy.Default.INJECTION);
    }
}
