/*
 * AbstractConstantActionFactory.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;

import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Skeleton implementation of a ConstantActionFactory.
 */
public abstract class AbstractConstantActionFactory implements ConstantActionFactory {
    @Nonnull
    @Override
    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate templateName, @Nonnull Options templateProperties) {
        return NoOpConstantActionFactory.INSTANCE.getCreateSchemaTemplateConstantAction(templateName, templateProperties);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull DatabaseTemplate template, @Nonnull Options constantActionOptions) {
        return NoOpConstantActionFactory.INSTANCE.getCreateDatabaseConstantAction(dbPath,template,constantActionOptions);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI schemaUrl, @Nonnull String templateId, Options constantActionOptions) {
        return NoOpConstantActionFactory.INSTANCE.getCreateSchemaConstantAction(schemaUrl, templateId, constantActionOptions);
    }
}
