/*
 * LimitConcurrencyExtension.java
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

package com.apple.test;

import com.google.auto.service.AutoService;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.concurrent.Semaphore;

@AutoService(Extension.class)
public class LimitConcurrencyExtension implements BeforeEachCallback, AfterEachCallback {
    static Semaphore testConcurrency = new Semaphore(Integer.parseInt(System.getProperty("tests.concurrencyLimit", "10")));

    @Override
    public void beforeEach(final ExtensionContext context) throws InterruptedException {
        testConcurrency.acquire();
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        testConcurrency.release();
    }
}
