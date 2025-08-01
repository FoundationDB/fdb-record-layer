/*
 * TransactionSetupsBlock.java
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

package com.apple.foundationdb.relational.yamltests.block;

import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import java.util.Map;

public class TransactionSetupsBlock {
    public static final String TRANSACTION_SETUP = "transaction_setups";

    public static Block parse(final int lineNumber,
                              final Object document,
                              final YamlExecutionContext executionContext) {
        final Map<?, ?> map = Matchers.map(document);
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final String transactionSetupName = Matchers.string(entry.getKey(), "transaction setup name");
            final String transactionSetupCommand = Matchers.string(entry.getValue(), "transaction setup command");
            executionContext.registerTransactionSetup(transactionSetupName, transactionSetupCommand);
        }
        return new NoOpBlock(lineNumber);
    }

}
