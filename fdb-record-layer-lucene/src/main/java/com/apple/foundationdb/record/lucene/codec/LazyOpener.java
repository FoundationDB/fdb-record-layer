package com.apple.foundationdb.record.lucene.codec;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.io.IOException;
import java.io.UncheckedIOException;

public class LazyOpener<T> {

    private final Supplier<T> opener;

    public LazyOpener(final Opener<T> opener) {
        this.opener = Suppliers.memoize(() -> {
            try {
                return opener.open();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public static <U> LazyOpener<U> supply(LazyOpener.Opener<U> opener) {
        return new LazyOpener<U>(opener);
    }

    public T get() throws IOException {
        try {
            return opener.get();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public T getUnchecked() {
        return opener.get();
    }

    public interface Opener<T> {
        T open() throws IOException;
    }
}
