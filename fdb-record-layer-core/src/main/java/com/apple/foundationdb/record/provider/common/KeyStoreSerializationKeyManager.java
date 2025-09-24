/*
 * KeyStoreSerializationKeyManager.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;


/**
 * A {@link SerializationKeyManager} that uses a {@link KeyStore}.
 * <p>
 * Key numbers are mapped to and from key names, which are looked up in the store.
 */
@API(API.Status.EXPERIMENTAL)
public class KeyStoreSerializationKeyManager implements SerializationKeyManager {
    @Nonnull
    private final KeyStore keyStore;
    @Nonnull
    private final KeyStore.ProtectionParameter keyEntryPassword;
    @Nonnull
    private final List<String> keyEntryAliases;
    private final int defaultKeyNumber;
    @Nonnull
    private final String cipherName;
    @Nonnull
    private final SecureRandom secureRandom;

    protected KeyStoreSerializationKeyManager(@Nonnull KeyStore keyStore, @Nonnull KeyStore.ProtectionParameter keyEntryPassword,
                                              @Nonnull List<String> keyEntryAliases, int defaultKeyNumber,
                                              @Nonnull String cipherName, @Nonnull SecureRandom secureRandom) {
        this.keyStore = keyStore;
        this.keyEntryPassword = keyEntryPassword;
        this.keyEntryAliases = keyEntryAliases;
        this.defaultKeyNumber = defaultKeyNumber;
        this.cipherName = cipherName;
        this.secureRandom = secureRandom;
    }

    @Override
    public int getSerializationKey() {
        return defaultKeyNumber;
    }

    @Override
    public Key getKey(int keyNumber) {
        if (keyNumber < 0 || keyNumber >= keyEntryAliases.size()) {
            throw new RecordSerializationException("key number out of range");
        }
        final String keyEntryAlias = keyEntryAliases.get(keyNumber);
        final KeyStore.SecretKeyEntry entry;
        try {
            entry = (KeyStore.SecretKeyEntry)keyStore.getEntry(keyEntryAlias, keyEntryPassword);
        } catch (GeneralSecurityException ex) {
            throw new RecordSerializationException("cannot load key", ex);
        }
        return entry.getSecretKey();
    }

    @Override
    public String getCipher(int keyNumber) {
        return cipherName;
    }

    @Override
    public Random getRandom(int keyNumber) {
        return secureRandom;
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        @Nullable
        String keyStoreFileName;
        @Nullable
        String keyStorePassword;
        @Nullable
        String keyEntryPassword;
        @Nullable
        String defaultKeyEntryAlias;
        @Nullable
        List<String> keyEntryAliases;
        @Nonnull
        String cipherName = CipherPool.DEFAULT_CIPHER;
        @Nullable
        SecureRandom secureRandom;

        protected Builder() {
        }

        @Nullable
        public String getKeyStoreFileName() {
            return keyStoreFileName;
        }

        public void setKeyStoreFileName(@Nonnull String keyStoreFileName) {
            this.keyStoreFileName = keyStoreFileName;
        }

        @Nullable
        public String getKeyStorePassword() {
            return keyStorePassword;
        }

        public void setKeyStorePassword(@Nonnull String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        @Nullable
        public String getKeyEntryPassword() {
            return keyEntryPassword;
        }

        public void setKeyEntryPassword(@Nonnull String keyEntryPassword) {
            this.keyEntryPassword = keyEntryPassword;
        }

        @Nullable
        public String getDefaultKeyEntryAlias() {
            return defaultKeyEntryAlias;
        }

        public void setDefaultKeyEntryAlias(@Nonnull String defaultKeyEntryAlias) {
            this.defaultKeyEntryAlias = defaultKeyEntryAlias;
        }

        @Nullable
        public List<String> getKeyEntryAliases() {
            return keyEntryAliases;
        }

        public void setKeyEntryAliases(@Nonnull List<String> keyEntryAliases) {
            this.keyEntryAliases = keyEntryAliases;
        }

        @Nonnull
        public String getCipherName() {
            return cipherName;
        }

        public void setCipherName(@Nonnull String cipherName) {
            this.cipherName = cipherName;
        }

        @Nullable
        public SecureRandom getSecureRandom() {
            return secureRandom;
        }

        public void setSecureRandom(@Nonnull SecureRandom secureRandom) {
            this.secureRandom = secureRandom;
        }

        @Nonnull
        public KeyStoreSerializationKeyManager build() {
            if (keyStoreFileName == null) {
                throw new RecordCoreArgumentException("must specify key store file name");
            }
            if (keyStorePassword == null) {
                keyStorePassword = "";
            }
            final KeyStore keyStore;
            try {
                keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(keyStoreFileName)) {
                    keyStore.load(fis, keyStorePassword.toCharArray());
                }
            } catch (FileNotFoundException ex) {
                throw new RecordCoreArgumentException("Key store not found", ex);
            } catch (GeneralSecurityException | IOException ex) {
                throw new RecordCoreArgumentException("Key store loading failed", ex);
            }
            if (keyEntryPassword == null) {
                keyEntryPassword = keyStorePassword;
            }
            final KeyStore.ProtectionParameter keyEntryProtection = new KeyStore.PasswordProtection(keyEntryPassword.toCharArray());
            if (keyEntryAliases == null && defaultKeyEntryAlias == null) {
                throw new RecordCoreArgumentException("must specify key alias list or single default alias");
            }
            final int defaultKeyNumber;
            if (keyEntryAliases == null) {
                keyEntryAliases = List.of(defaultKeyEntryAlias);
                defaultKeyNumber = 0;
            } else if (defaultKeyEntryAlias == null) {
                defaultKeyNumber = keyEntryAliases.size() - 1;
            } else {
                defaultKeyNumber = keyEntryAliases.indexOf(defaultKeyEntryAlias);
                if (defaultKeyNumber < 0) {
                    throw new RecordCoreArgumentException("default key alias not in key alias list");
                }
            }
            if (secureRandom == null) {
                secureRandom = new SecureRandom();
            }
            return new KeyStoreSerializationKeyManager(keyStore, keyEntryProtection, keyEntryAliases, defaultKeyNumber,
                                                       cipherName, secureRandom);
        }
    }
}
