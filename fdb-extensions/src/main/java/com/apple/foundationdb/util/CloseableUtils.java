/*
 * CloseableUtils.java
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

package com.apple.foundationdb.util;

import com.apple.foundationdb.annotation.API;

/**
 * Utility methods to help interact with {@link AutoCloseable} classes.
 **/
public class CloseableUtils {
    /**
     * A utility to close multiple {@link AutoCloseable} objects, preserving all the caught exceptions.
     * The method would attempt to close all closeables in order, even if some failed.
     * Note that {@link CloseException} is used to wrap any exception thrown during the closing process. The reason for
     * that is the compiler fails to compile a {@link AutoCloseable#close()} implementation that throws a generic
     * {@link Exception} (due to {@link InterruptedException} issue) - We therefore have to catch and wrap all exceptions.
     * @param closeables the given sequence of {@link AutoCloseable}
     * @throws CloseException in case any exception was caught during the process. The first exception will be added
     * as a {@code cause}. In case more than one exception was caught, it will be added as Suppressed.
     */
    @API(API.Status.INTERNAL)
    @SuppressWarnings("PMD.CloseResource")
    public static void closeAll(AutoCloseable... closeables) throws CloseException {
        CloseException accumulatedException = null;
        for (AutoCloseable closeable: closeables) {
            try {
                closeable.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (accumulatedException == null) {
                    accumulatedException = new CloseException(e);
                } else {
                    accumulatedException.addSuppressed(e);
                }
            } catch (Exception e) {
                if (accumulatedException == null) {
                    accumulatedException = new CloseException(e);
                } else {
                    accumulatedException.addSuppressed(e);
                }
            }
        }
        if (accumulatedException != null) {
            throw accumulatedException;
        }
    }

    private CloseableUtils() {
        // prevent constructor from being called
    }
}
