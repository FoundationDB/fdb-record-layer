/*
 * CommandUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;

import com.google.protobuf.Descriptors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

/**
 * Util class for yaml-tests commands.
 */
public class CommandUtil {
    /**
     * Create a SchemaTemplate object from .proto
     *
     * @param loadCommandString input format is: "load: schema template ${SCHEMA_TEMPLATE_NAME} from ${PROTO_CLASS_NAME}"
     * @return a SchemaTemplate object
     */
    public static SchemaTemplate fromProto(String loadCommandString) {
        RecordMetaData metaData;
        Pair<String, String> templateNameAndProtoClassName = parseLoadCommandString(loadCommandString);
        try {
            Class<?> act = Class.forName(templateNameAndProtoClassName.getRight());
            Method method = act.getMethod("getDescriptor");
            Descriptors.FileDescriptor o = (Descriptors.FileDescriptor) method.invoke(null);
            metaData = RecordMetaData.build(o);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return RecordLayerSchemaTemplate.fromRecordMetadata(metaData, templateNameAndProtoClassName.getLeft(), 1);
    }

    private static Pair<String, String> parseLoadCommandString(String loadCommandString) {
        StringTokenizer lcsTokenizer = new StringTokenizer(loadCommandString, " ");
        if (lcsTokenizer.countTokens() != 5) {
            Assertions.fail("Expecting load schema template command consisting of 5 tokens");
        }
        if (!"schema".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load schema template command to start with schema template");
        }
        if (!"template".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load schema template command to start with schema template");
        }
        String templateName = lcsTokenizer.nextToken();
        if (!"from".equals(lcsTokenizer.nextToken())) {
            Assertions.fail("Expecting load schema template command to start with schema template");
        }
        String protoClassName = lcsTokenizer.nextToken();
        return new ImmutablePair<>(templateName, protoClassName);
    }
}
