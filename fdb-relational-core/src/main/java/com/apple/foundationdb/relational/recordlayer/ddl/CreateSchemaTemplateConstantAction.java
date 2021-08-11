/*
 * CreateSchemaTemplateConstantAction.java
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

import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import javax.annotation.Nonnull;
import java.net.URI;

public class CreateSchemaTemplateConstantAction implements ConstantAction {
    private final SchemaTemplate template;
    private final MutableRecordMetaDataStore metaDataStore;
    private final URI baseTemplatePath;

    public CreateSchemaTemplateConstantAction(@Nonnull URI baseTemplatePath, @Nonnull  SchemaTemplate template,@Nonnull MutableRecordMetaDataStore metaDataStore) {
        this.template = template;
        this.metaDataStore = metaDataStore;
        this.baseTemplatePath = baseTemplatePath;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        final String path = KeySpaceUtils.getPath(template.getUniqueName());
        String templatePath = path.startsWith("/") ? baseTemplatePath + path : baseTemplatePath + "/" + path;
        URI templateUri = URI.create(templatePath);

        assert template instanceof RecordMetaDataProvider: "Cannot use this constant action with SchemaTemplate of type <"+template.getClass()+">";


        metaDataStore.setSchemaTemplateMetaData(templateUri,((RecordMetaDataProvider) template));
    }
}
