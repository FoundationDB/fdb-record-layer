/*
 * TaskNotifyingExecutor.java
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;

/**
 * An executor that can perform pre- and post- work in the context of the thread executing a task. A typical use for
 * this executor is to perform actions such as setting up and tearing down thread local variables that need to be
 * present for code that is executed within the tasks.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class TaskNotifyingExecutor implements Executor {
    @Nonnull
    protected final Executor delegate;

    public TaskNotifyingExecutor(@Nonnull Executor executor) {
        this.delegate = executor;
    }

    @Override
    public void execute(final Runnable task) {
        if (task instanceof Notifier) {
            delegate.execute(task);
        } else {
            delegate.execute(new Notifier( task));
        }
    }

    /**
     * Called in the context of an executor thread, immediately prior to actually executing a task. Exceptions
     * thrown from this method will prevent the execution of the task itself, however the {@link #afterTask()}
     * will still be executed.
     */
    public abstract void beforeTask();

    /**
     * Called in the context of an executor thread, immediately after a task has completed execution (either
     * successfully or with error). Exceptions thrown from this will cause the task to be treated as a failure
     * and will mask any exception that the task may have produced.
     */
    public abstract void afterTask();

    private class Notifier implements Runnable {
        @Nonnull
        private final Runnable delegate;

        public Notifier(@Nonnull Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            try {
                beforeTask();
                delegate.run();
            } finally {
                afterTask();
            }
        }
    }
}
