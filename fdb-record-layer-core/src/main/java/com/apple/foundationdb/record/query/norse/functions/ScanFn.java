/*
 * ScanFn.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.norse.BuiltInFunction;
import com.apple.foundationdb.record.query.norse.ParserContext;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.Atom;
import com.apple.foundationdb.record.query.predicates.Type;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * Function
 * scan(STRING...) -> STREAM.
 */
@AutoService(BuiltInFunction.class)
public class ScanFn extends BuiltInFunction<RelationalExpression> {
    public ScanFn() {
        super("scan",
                ImmutableList.of(), Type.primitiveType(Type.TypeCode.STRING), ScanFn::encapsulate);
    }

    private static RelationalExpression encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<RelationalExpression> builtInFunction, @Nonnull final List<Atom> arguments) {
        final RecordMetaData recordMetaData = parserContext.getRecordMetaData();
        final Set<String> allAvailableRecordTypes = recordMetaData.getRecordTypes().keySet();

        return new RecordQueryScanPlan(allAvailableRecordTypes,
                ScanComparisons.EMPTY, false, false);
    }
}
