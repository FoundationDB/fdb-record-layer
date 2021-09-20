/*
 * FilterFn.java
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

package com.apple.foundationdb.record.query.norse.functions;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Function
 * filter(STRING...) -> RELATION.
 */
@AutoService(BuiltInFunction.class)
public class FromFn extends BuiltInFunction<RelationalExpression> {
    public FromFn() {
        super("from",
                ImmutableList.of(), Type.primitiveType(Type.TypeCode.STRING), FromFn::encapsulate);
    }

    private static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Atom> arguments) {
        // force evaluation of the string-type arguments (for the record types)
        final ImmutableSet<String> recordTypeNames = arguments
                .stream()
                .peek(argument -> Verify.verify(argument.getResultType().getTypeCode() == Type.TypeCode.STRING))
                .map(argument -> {
                    final Object result = ((Value)argument).compileTimeEval(EvaluationContext.EMPTY);
                    if (result instanceof String) {
                        return (String)result;
                    } else {
                        throw new IllegalArgumentException("arguments to from() must be strings (for record types)");
                    }
                })
                .collect(ImmutableSet.toImmutableSet());

        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();

        final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = recordTypeNames
                .stream()
                .flatMap(recordTypeName -> recordMetaData.getRecordType(recordTypeName).getDescriptor().getFields().stream())
                .collect(Collectors.groupingBy(Descriptors.FieldDescriptor::getName,
                        Collectors.reducing(null,
                                (fieldDescriptor, fieldDescriptor2) -> {
                                    Verify.verify(fieldDescriptor != null || fieldDescriptor2 != null);
                                    if (fieldDescriptor == null) {
                                        return fieldDescriptor2;
                                    }
                                    if (fieldDescriptor2 == null) {
                                        return fieldDescriptor;
                                    }
                                    // TODO improve
                                    final Type.TypeCode typeCode = Type.TypeCode.fromProtobufType(fieldDescriptor.getType());
                                    final Type.TypeCode typeCode2 = Type.TypeCode.fromProtobufType(fieldDescriptor2.getType());
                                    if (typeCode.isPrimitive() &&
                                            typeCode2.isPrimitive() &&
                                            typeCode == typeCode2) {
                                        return fieldDescriptor;
                                    }

                                    throw new IllegalArgumentException("cannot form union type of complex fields");
                                })));

        final Set<String> allAvailableRecordTypes = recordMetaData.getRecordTypes().keySet();
        return new LogicalTypeFilterExpression(recordTypeNames, new FullUnorderedScanExpression(allAvailableRecordTypes, fieldDescriptorMap));
    }
}
