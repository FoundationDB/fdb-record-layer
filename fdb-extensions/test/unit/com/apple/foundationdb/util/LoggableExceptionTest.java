/*
 * LoggableExceptionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link LoggableException}.
 */
public class LoggableExceptionTest {
    @Test
    public void emptyLogInfo() {
        LoggableException e = new LoggableException("this has no k/v pairs");
        Map<String,Object> logInfo = e.getLogInfo();
        assertNotNull(logInfo);
        assertEquals(Collections.emptyMap(), logInfo);
        Object[] logInfoArray = e.exportLogInfo();
        assertNotNull(logInfoArray);
        assertEquals(0, logInfoArray.length);
    }

    @Test
    public void oneLogInfo() {
        LoggableException e = new LoggableException("this has one k/v pair")
                .addLogInfo("key", "value");
        Map<String,Object> logInfo = e.getLogInfo();
        assertNotNull(logInfo);
        assertEquals(Collections.singletonMap("key", "value"), logInfo);
        Object[] logInfoArray = e.exportLogInfo();
        assertNotNull(logInfoArray);
        assertArrayEquals(new Object[]{"key", "value"}, logInfoArray);
    }

    @Test
    public void multipleLogInfo() {
        Map<String,Object> expectedLogInfo = new HashMap<>();
        expectedLogInfo.put("k0", "v0");
        expectedLogInfo.put("k1", "v1");
        expectedLogInfo.put("k2", "v2");
        LoggableException e = new LoggableException("this has multiple k/v pairs", "k0", "v0", "k1", "v1", "k2", "v2");
        Map<String,Object> logInfo = e.getLogInfo();
        assertNotNull(logInfo);
        assertEquals(expectedLogInfo, logInfo);
        Object[] logInfoArray = e.exportLogInfo();
        assertEquals(6, logInfoArray.length);
        Set<Object> logInfoKeys = new HashSet<>(Arrays.asList(logInfoArray[0], logInfoArray[2], logInfoArray[4]));
        assertEquals(expectedLogInfo.keySet(), logInfoKeys);
        for (int i = 0; i < logInfoArray.length; i += 2) {
            assertEquals(expectedLogInfo.get(String.valueOf(logInfoArray[i])), logInfoArray[i + 1]);
        }
    }

    @Test
    public void oddLogInfoValues() {
        assertThrows(IllegalArgumentException.class, () -> new LoggableException("odd number of log info!", "k1", "v1", "k2"));
        assertThrows(IllegalArgumentException.class, () -> new LoggableException("odd in call!").addLogInfo("k1", "v1", "k2"));
    }
}
