package com.apple.foundationdb.record.provider.foundationdb;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class TransactionalRunner implements AutoCloseable {

    private final FDBDatabase database;
    private final FDBRecordContextConfig.Builder contextConfigBuilder;
    private boolean closed;
    private List<FDBRecordContext> contextsToClose;

    public TransactionalRunner(@Nonnull FDBDatabase database,
                               FDBRecordContextConfig.Builder contextConfigBuilder) {
        this.database = database;
        this.contextConfigBuilder = contextConfigBuilder;

        contextsToClose = new ArrayList<>();
    }

    public <T> CompletableFuture<T> runAsync(final boolean clearWeakReadSemantics,
                                             Function<FDBRecordContext, CompletableFuture<T>> runnable) {
        FDBRecordContext context = openContext(clearWeakReadSemantics);
        return runnable.apply(context).thenCompose(val ->
                context.commitAsync().thenApply( vignore -> val)
                        .whenComplete((hasMore, exception) -> {
                            context.close();
                        })
        );
    }

    @Nonnull
    public FDBRecordContext openContext(boolean initialAttempt) {
        if (closed) {
            throw new FDBDatabaseRunner.RunnerClosed();
        }
        FDBRecordContextConfig contextConfig;
        if (initialAttempt || contextConfigBuilder.getWeakReadSemantics() == null) {
            contextConfig = contextConfigBuilder.build();
        } else {
            // Clear any weak semantics after first attempt.
            contextConfig = contextConfigBuilder.copyBuilder().setWeakReadSemantics(null).build();
        }
        FDBRecordContext context = database.openContext(contextConfig);
        addContextToClose(context);
        return context;
    }

    private synchronized void addContextToClose(@Nonnull FDBRecordContext context) {
        if (closed) {
            context.close();
            throw new FDBDatabaseRunner.RunnerClosed();
        }
        contextsToClose.removeIf(FDBRecordContext::isClosed);
        contextsToClose.add(context);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        contextsToClose.forEach(FDBRecordContext::close);
        this.closed = true;
    }

}
