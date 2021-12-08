/*
 * RecordLayerPropertyTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.properties;

import com.apple.foundationdb.record.RecordCoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Objects;

public class RecordLayerPropertyTest {
    @Test
    void testDefaultPropertyValue() {
        final RecordLayerPropertyKey<Boolean> propertyKey = getTestPropertyKey();
        // Build the property storage without configuring the prop with a value supplier
        final RecordLayerPropertyStorage properties = RecordLayerPropertyStorage.newBuilder()
                .build();
        // Assert the prop value is returned as default value false
        Assertions.assertEquals(false, Objects.requireNonNull(properties.getPropertyValue(propertyKey)), "Unexpected prop value");
    }

    @Test
    void testPropertyValueFromSupplier() {
        final RecordLayerPropertyKey<Boolean> propertyKey = getTestPropertyKey();
        // Build the property storage with configuring the prop with a value supplier
        final RecordLayerPropertyStorage properties = RecordLayerPropertyStorage.newBuilder()
                .addProp(propertyKey, true)
                .build();
        // Assert the prop value is returned from supplier as true
        Assertions.assertEquals(true, Objects.requireNonNull(properties.getPropertyValue(propertyKey)), "Unexpected prop value");
    }

    @Test
    void testInvalidPropType() {
        final RecordLayerPropertyKey<Boolean> propertyKey = getTestPropertyKey();
        final String propName = propertyKey.getName();
        // Build the property storage with the boolean prop using a String type value
        final RecordLayerPropertyStorage properties = RecordLayerPropertyStorage.newBuilder()
                .addProp(RecordLayerPropertyKey.stringPropertyKey(propName, "invalidStringForBooleanProp"), "invalidStringForBooleanProp")
                .build();
        // Assert exception is thrown due to casting a String to Boolean
        Assertions.assertThrows(RecordCoreException.class,
                () -> Objects.requireNonNull(properties.getPropertyValue(propertyKey)), "A class cast exception from putting a string in a boolean property is expected");
    }

    private static RecordLayerPropertyKey<Boolean> getTestPropertyKey() {
        return RecordLayerPropertyKey.booleanPropertyKey("testProp", false);
    }
}
