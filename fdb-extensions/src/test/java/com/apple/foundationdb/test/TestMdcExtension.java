/*
 * TestMdcExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

import com.google.auto.service.AutoService;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.MDC;

/**
 * Extension to populate the logging MDC with information regarding the test class and method. This ensures that
 * the test information is included in logs during the test.
 */
@AutoService(Extension.class)
public class TestMdcExtension implements BeforeEachCallback, AfterEachCallback {
    public static final String TEST_CLASS = "test_class";
    public static final String TEST_METHOD = "test_method";
    public static final String TEST_DISPLAY_NAME = "test_display_name";

    @Override
    public void beforeEach(final ExtensionContext extensionContext) {
        MDC.put(TEST_DISPLAY_NAME, extensionContext.getDisplayName());
        extensionContext.getTestMethod().ifPresent(method -> {
            MDC.put(TEST_CLASS, method.getDeclaringClass().getName());
            MDC.put(TEST_METHOD, method.getName());
        });
    }

    @Override
    public void afterEach(final ExtensionContext extensionContext) {
        MDC.remove(TEST_METHOD);
        MDC.remove(TEST_CLASS);
        MDC.remove(TEST_DISPLAY_NAME);
    }
}
