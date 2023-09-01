package com.apple.foundationdb.record.lucene.codec;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LazyCloseable<T extends Closeable> implements Closeable {
    private final LazyOpener<T> opener;
    private final AtomicBoolean initialized;

    public static <U extends Closeable> LazyCloseable<U> supply(LazyOpener.Opener<U> opener) {
        return new LazyCloseable<U>(opener);
    }

    public LazyCloseable(LazyOpener.Opener<T> opener) {
        this.initialized = new AtomicBoolean(true);
        this.opener = new LazyOpener<>(() -> {
            final T result = opener.open();
            initialized.set(true);
            return result;
        });
    }

    public T get() throws IOException {
        return opener.get();
    }

    public T getUnchecked() {
        return opener.getUnchecked();
    }

    @Override
    public void close() throws IOException {
        if (initialized.get()) {
            opener.get().close();
        }
    }

}
