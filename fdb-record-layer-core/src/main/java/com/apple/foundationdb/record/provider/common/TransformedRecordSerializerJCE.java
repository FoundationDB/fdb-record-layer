/*
 * TransformedRecordSerializerJCE.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.SecureRandom;

/**
 * An extension of {@link TransformedRecordSerializer} to use JCE to encrypt and decrypt records.
 * @param <M> type of {@link Message} that underlying records will use
 */
@API(API.Status.UNSTABLE)
public class TransformedRecordSerializerJCE<M extends Message> extends TransformedRecordSerializer<M> {
    @Nullable
    protected final SerializationKeyManager keyManager;

    protected TransformedRecordSerializerJCE(@Nonnull RecordSerializer<M> inner,
                                             boolean compressWhenSerializing,
                                             int compressionLevel,
                                             boolean encryptWhenSerializing,
                                             double writeValidationRatio,
                                             @Nullable SerializationKeyManager keyManager) {
        super(inner, compressWhenSerializing, compressionLevel, encryptWhenSerializing, writeValidationRatio);
        this.keyManager = keyManager;
    }

    @Override
    protected void encrypt(@Nonnull TransformedRecordSerializerState state, @Nullable StoreTimer timer) throws GeneralSecurityException {
        if (keyManager == null) {
            throw new RecordSerializationException("attempted to encrypt without setting key manager (cipher name and key)");
        }
        long startTime = System.nanoTime();

        int keyNumber = keyManager.getSerializationKey();
        state.setKeyNumber(keyNumber);

        byte[] ivData = new byte[CipherPool.IV_SIZE];
        keyManager.getRandom(keyNumber).nextBytes(ivData);
        IvParameterSpec iv = new IvParameterSpec(ivData);
        Cipher cipher = CipherPool.borrowCipher(keyManager.getCipher(keyNumber));
        try {
            cipher.init(Cipher.ENCRYPT_MODE, keyManager.getKey(keyNumber), iv);

            byte[] plainText = state.getDataArray();
            byte[] cipherText = cipher.doFinal(plainText);

            int totalSize = CipherPool.IV_SIZE + cipherText.length;
            byte[] serialized = new byte[totalSize];
            System.arraycopy(iv.getIV(), 0, serialized, 0, CipherPool.IV_SIZE);
            System.arraycopy(cipherText, 0, serialized, CipherPool.IV_SIZE, cipherText.length);
            state.setEncrypted(true);
            state.setDataArray(serialized);
        } finally {
            CipherPool.returnCipher(cipher);
            if (timer != null) {
                timer.recordSinceNanoTime(Events.ENCRYPT_SERIALIZED_RECORD, startTime);
            }
        }
    }

    @Override
    protected void decrypt(@Nonnull TransformedRecordSerializerState state, @Nullable StoreTimer timer) throws GeneralSecurityException {
        if (keyManager == null) {
            throw new RecordSerializationException("missing encryption key or provider during decryption");
        }
        long startTime = System.nanoTime();

        byte[] ivData = new byte[CipherPool.IV_SIZE];
        System.arraycopy(state.getData(), state.getOffset(), ivData, 0, CipherPool.IV_SIZE);
        IvParameterSpec iv = new IvParameterSpec(ivData);

        byte[] cipherText = new byte[state.getLength() - CipherPool.IV_SIZE];
        System.arraycopy(state.getData(), state.getOffset() + CipherPool.IV_SIZE, cipherText, 0, cipherText.length);
        Cipher cipher = CipherPool.borrowCipher(keyManager.getCipher(state.getKeyNumber()));
        try {
            cipher.init(Cipher.DECRYPT_MODE, keyManager.getKey(state.getKeyNumber()), iv);

            byte[] plainText = cipher.doFinal(cipherText);
            state.setDataArray(plainText);
        } finally {
            CipherPool.returnCipher(cipher);
            if (timer != null) {
                timer.recordSinceNanoTime(Events.DECRYPT_SERIALIZED_RECORD, startTime);
            }
        }
    }

    @Nonnull
    @Override
    public RecordSerializer<Message> widen() {
        return new TransformedRecordSerializerJCE<>(inner.widen(), compressWhenSerializing, compressionLevel, encryptWhenSerializing, writeValidationRatio, keyManager);
    }

    /**
     * Creates a new {@link Builder TransformedRecordSerializerJCE.Builder} instance
     * that is backed by the default serializer for {@link Message}s, namely
     * a {@link DynamicMessageRecordSerializer}. Methods on the returned
     * <code>Builder</code> instance can be used to specify which transformations
     * to apply after using the default serializer.
     *
     * @return <code>Builder</code> instance backed by a {@link DynamicMessageRecordSerializer}
     */
    public static Builder<Message> newDefaultBuilder() {
        return newBuilder(DynamicMessageRecordSerializer.instance());
    }

    /**
     * Creates a new {@link Builder TransformedRecordSerializerJCE.Builder} instance around
     * the given serializer. Methods on the <code>Builder</code> instance can be used to
     * specify which transformations after using the provided serializer.
     *
     * @param inner {@link RecordSerializer} to use before/after applying transformations
     * @param <M> type of {@link Message} that underlying records will use
     * @return <code>Builder</code> instance that can be used to specify transformations
     */
    public static <M extends Message> Builder<M> newBuilder(@Nonnull RecordSerializer<M> inner) {
        return new Builder<>(inner);
    }

    /**
     * Builder class for the {@link TransformedRecordSerializerJCE} class. The methods
     * on this builder allows the user to specify parameters that can then be
     * used to specify which transformations should be applied before serializing
     * the record. It can also be used to specify parameters that will then be
     * applied to serialized data before deserializing, though the prefix of
     * records themselves are what specify the needed transformations.
     *
     * @param <M> type of {@link Message} that underlying records will use
     */
    public static class Builder<M extends Message> extends TransformedRecordSerializer.Builder<M> {
        @Nullable
        protected SerializationKeyManager keyManager;
        @Nullable
        protected String cipherName;
        @Nullable
        protected Key encryptionKey;
        @Nullable
        protected SecureRandom secureRandom;

        protected Builder(@Nonnull RecordSerializer<M> inner) {
            super(inner);
        }

        @Override
        @Nonnull
        public Builder<M> setCompressWhenSerializing(boolean compressWhenSerializing) {
            super.setCompressWhenSerializing(compressWhenSerializing);
            return this;
        }

        @Override
        @Nonnull
        public Builder<M> setCompressionLevel(int level) {
            super.setCompressionLevel(level);
            return this;
        }

        /**
         * Whether to encrypt records after serializing. Should
         * compression and encryption both be set, then data
         * will be compressed before they are encrypted.
         * If enabled, the user must also call {@link #setEncryptionKey(Key)}
         * to specify the encryption key.
         * @param encryptWhenSerializing <code>true</code> if records should be encrypted and <code>false</code> otherwise
         * @return this <code>Builder</code>
         */
        @Override
        @Nonnull
        public Builder<M> setEncryptWhenSerializing(boolean encryptWhenSerializing) {
            super.setEncryptWhenSerializing(encryptWhenSerializing);
            return this;
        }

        /**
         * {@inheritDoc}
         *
         * @param writeValidationRatio what ratio of record serializations should be validated
         * @return this <code>Builder</code>
         */
        @Override
        @Nonnull
        public Builder<M> setWriteValidationRatio(final double writeValidationRatio) {
            super.setWriteValidationRatio(writeValidationRatio);
            return this;
        }

        /**
         * Specifies the encryption key used to encrypt or
         * decrypt a record. If this is not specified and encryption
         * when serializing is enabled, then this builder will fail
         * to build. If this is not specified, and records are not
         * to be encrypted when serialized but an encrypted record
         * is encountered when attempting to deserialize a record,
         * then deserialization will fail with a
         * {@link RecordSerializationException}.
         * @param encryptionKey key to supply to encryption method
         * @return this <code>Builder</code>
         */
        public Builder<M> setEncryptionKey(@Nonnull Key encryptionKey) {
            this.encryptionKey = encryptionKey;
            return this;
        }

        /**
         * Specifies the cipher algorithm used to encrypt or
         * decrypt a record. If this is not specified, the default
         * is <code>AES/CBC/PKCS5Padding</code>.
         * @param cipherName name of the cipher algorithm to use
         * @return this <code>Builder</code>
         */
        public Builder<M> setCipherName(@Nonnull String cipherName) {
            this.cipherName = cipherName;
            return this;
        }

        /**
         * Clears a previously specified key and provider that
         * might have been passed to this <code>Builder</code>.
         * @return this <code>Builder</code>
         */
        public Builder<M> clearEncryption() {
            this.cipherName = null;
            this.encryptionKey = null;
            return this;
        }

        /**
         * Sets the secure random number generator used during
         * cryptographic operations. If none is specified but
         * encryption is enabled, then a new one will be created
         * when the {@link TransformedRecordSerializerJCE} is built.
         * @param secureRandom a secure random number generator
         * @return this <code>Builder</code>
         */
        public Builder<M> setSecureRandom(@Nonnull SecureRandom secureRandom) {
            this.secureRandom = secureRandom;
            return this;
        }

        /**
         * Clears a previously set secure random number generator
         * that might have been passed to this <code>Builder</code>.
         * @return this <code>Builder</code>
         */
        public Builder<M> clearSecureRandom() {
            this.secureRandom = null;
            return this;
        }

        /**
         * Sets the key manager used during cryptographic operations.
         * @param keyManager key manager to use for encrypting and decrypting
         * @return this <code>Builder</code>
         */
        public Builder<M> setKeyManager(@Nonnull SerializationKeyManager keyManager) {
            this.keyManager = keyManager;
            return this;
        }

        /**
         * Clears a previously set key manager
         * that might have been passed to this <code>Builder</code>.
         * @return this <code>Builder</code>
         */
        public Builder<M> clearKeyManager() {
            this.keyManager = null;
            return this;
        }

        /**
         * Construct a {@link TransformedRecordSerializerJCE} from the
         * parameters specified by this builder. If one has enabled
         * encryption at serialization time, then this will fail
         * with an {@link RecordCoreArgumentException}.
         * @return a {@link TransformedRecordSerializerJCE} from this <code>Builder</code>
         * @throws RecordCoreArgumentException if encryption is enabled when serializing but no encryption is specified
         */
        @Override
        public TransformedRecordSerializerJCE<M> build() {
            if (keyManager == null) {
                if (encryptionKey != null) {
                    keyManager = new FixedZeroKeyManager(encryptionKey, cipherName, secureRandom);
                } else if (encryptWhenSerializing) {
                    throw new RecordCoreArgumentException("cannot encrypt when serializing if encryption key is not set");
                }
            } else {
                if (encryptionKey != null) {
                    throw new RecordCoreArgumentException("cannot specify both key manager and encryption key");
                }
                if (cipherName != null) {
                    throw new RecordCoreArgumentException("cannot specify both key manager and cipher name");
                }
            }
            return new TransformedRecordSerializerJCE<>(
                    inner,
                    compressWhenSerializing,
                    compressionLevel,
                    encryptWhenSerializing,
                    writeValidationRatio,
                    keyManager
            );
        }

    }

}
