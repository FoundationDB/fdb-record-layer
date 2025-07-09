/*
 * ReplRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClasspathRoots;

/**
 * The REPL runner.
 */
public class ReplRunner {
    public void run(@Nullable final String className) {
        Set<Path> classpathRoots = new HashSet<>();
        classpathRoots.add(Path.of(".out/classes/java/test")); // test output

        try {
            LauncherDiscoveryRequestBuilder requestBuilder = LauncherDiscoveryRequestBuilder.request();
            if (className != null) {
                requestBuilder.selectors(DiscoverySelectors.selectClass(className));
            } else {
                requestBuilder.selectors(selectClasspathRoots(classpathRoots))
                        .filters(TagFilter.includeTags("debug"));
            }
            LauncherDiscoveryRequest request = requestBuilder.build();
            Launcher launcher = LauncherFactory.create();
            launcher.discover(request);
            SummaryGeneratingListener listener = new SummaryGeneratingListener();
            launcher.registerTestExecutionListeners(new DebugListener(), listener);
            launcher.execute(request);
            listener.getSummary().printTo(new PrintWriter(System.out));
        } catch (final Throwable t) {
            //noinspection CallToPrintStackTrace
            t.printStackTrace();
        }
    }

    public static void main(String[] arguments) {
        final var argumentsMap = parseCommandLine(arguments);
        new ReplRunner().run(argumentsMap.get("clazz"));
    }

    private static Map<String, String> parseCommandLine(String[] arguments) {
        final var resultBuilder = ImmutableMap.<String, String>builder();
        for (final var arg : arguments) {
            final var splitArg = arg.split("=");
            Preconditions.checkArgument(splitArg.length == 2);
            resultBuilder.put(splitArg[0].substring(2), splitArg[1]);
        }
        return resultBuilder.build();
    }

    private static class DebugListener implements TestExecutionListener {
        @Override
        public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
            if (testExecutionResult.getStatus() == TestExecutionResult.Status.FAILED) {
                System.out.println("FAILED: " + testIdentifier.getDisplayName());
                testExecutionResult.getThrowable().ifPresent(Throwable::printStackTrace);
            }
        }
    }
}
