/*
 * ContextRestoringExecutor.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.TaskNotifyingExecutor;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * An executor that ensures that the {@link MDC} from the thread that creates the {@code FDBRecordContext} is
 * available to all completion tasks produced from it.
 */
class ContextRestoringExecutor extends TaskNotifyingExecutor {
    @Nonnull
    private final Map<String, String> mdcContext;

    public ContextRestoringExecutor(@Nonnull Executor delegate, @Nonnull Map<String, String> mdcContext) {
        super(delegate);
        this.mdcContext = mdcContext;
    }

    @Override
    public void beforeTask() {
        restoreMdc(mdcContext);
    }

    @Override
    public void afterTask() {
        clearMdc(mdcContext);
    }

    @Nonnull
    public Map<String, String> getMdcContext() {
        return mdcContext;
    }

    static void restoreMdc(@Nonnull Map<String, String> mdcContext) {
        MDC.setContextMap(mdcContext);
    }

    static void clearMdc(@Nonnull Map<String, String> mdcContext) {
        Map<String, String> map = MDC.getMDCAdapter().getCopyOfContextMap();
        for (String key : mdcContext.keySet()) {
            map.remove(key);
        }
        MDC.setContextMap(map);
    }
}
