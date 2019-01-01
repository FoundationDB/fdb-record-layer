/*
 * SpatialFunctionKeyExpressionFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.geophile;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

/**
 * Implemention of (geo-)spatial index key functions.
 * @see SpatialFunctionNames
 */
@AutoService(FunctionKeyExpression.Factory.class)
@API(API.Status.EXPERIMENTAL)
public class SpatialFunctionKeyExpressionFactory implements FunctionKeyExpression.Factory {
    @Nonnull
    @Override
    public List<FunctionKeyExpression.Builder> getBuilders() {
        return Arrays.asList(
                new FunctionKeyExpression.BiFunctionBuilder(SpatialFunctionNames.GEO_POINT_Z, SpatialFunctionKeyExpression.GeoPointZ::new),
                new FunctionKeyExpression.BiFunctionBuilder(SpatialFunctionNames.GEO_JSON_Z, SpatialFunctionKeyExpression.GeoJsonZ::new),
                new FunctionKeyExpression.BiFunctionBuilder(SpatialFunctionNames.GEO_WKB_Z, SpatialFunctionKeyExpression.GeoWKBZ::new),
                new FunctionKeyExpression.BiFunctionBuilder(SpatialFunctionNames.GEO_WKT_Z, SpatialFunctionKeyExpression.GeoWKTZ::new)
        );
    }
}
