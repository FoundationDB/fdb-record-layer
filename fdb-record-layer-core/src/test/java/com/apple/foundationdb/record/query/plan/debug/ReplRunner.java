/*
 * ReplRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.discovery.MethodSelector;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The REPL runner.
 */
public class ReplRunner {
    SummaryGeneratingListener listener = new SummaryGeneratingListener();

    public void run(@Nonnull final String className, @Nonnull final String testMethodName, @Nullable final String paramTypes) {
        Debugger.setDebugger(new PlannerRepl());
        Debugger.setup();

        final MethodSelector methodSelector;
        if (paramTypes == null) {
            methodSelector = DiscoverySelectors.selectMethod(
                    className,
                    testMethodName);
        } else {
            methodSelector = DiscoverySelectors.selectMethod(
                    className,
                    testMethodName,
                    paramTypes);
        }

        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                //.selectors(selectClass(FDBOrQueryToUnionTest.class))
                .selectors(methodSelector)
                .build();
        Launcher launcher = LauncherFactory.create();
        launcher.discover(request);
        launcher.registerTestExecutionListeners(listener);
        try {
            launcher.execute(request);
        } catch (final Throwable t) {
            t.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: plannerRepl.sh fully.qualified.className testMethodName [parameter_type(,parameter_type)*]");
            System.exit(1);
        }

        if (args.length == 2) {
            new ReplRunner().run(args[0], args[1], null);
        } else {
            new ReplRunner().run(args[0], args[1], args[2]);
        }
    }
}
