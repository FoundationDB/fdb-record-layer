/*
 * TestClassSubspaceExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

import com.apple.foundationdb.subspace.Subspace;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;

/**
 * Variant of {@link TestSubspaceExtension} that can be used to create a test subspace that is shared by
 * all tests in a given class. This will ensure that the subspace is cleared out at the conclusion of
 * all tests have completed, whereas the {@link TestSubspaceExtension} clears out the subspace at the end
 * of every test.
 */
public class TestClassSubspaceExtension implements AfterAllCallback {
    @Nonnull
    private final TestSubspaceExtension subspaceExtension;

    public TestClassSubspaceExtension(@Nonnull TestDatabaseExtension dbExtension) {
        this.subspaceExtension = new TestSubspaceExtension(dbExtension);
    }

    @Nonnull
    public Subspace getSubspace() {
        return subspaceExtension.getSubspace();
    }

    @Override
    public void afterAll(final ExtensionContext extensionContext) {
        // Call the normal extension's afterEach here in the afterAll. That way, the subspace will get
        // cleared out, but only at the conclusion of all tests in the test class instead of after each test
        subspaceExtension.afterEach(extensionContext);
    }
}
