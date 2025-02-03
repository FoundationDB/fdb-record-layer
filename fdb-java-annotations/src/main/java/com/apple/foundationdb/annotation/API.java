/*
 * API.java
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

package com.apple.foundationdb.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation used on public types, fields, and methods to indicate their level of stability for consumers of the API.
 *
 * <p>
 * If a class or interface is annotated with {@code API}, all of its fields and methods are considered to have that same
 * level of stability by default. However, this may be changed by annotating a member explicitly.
 * </p>
 *
 * <p>
 * An API may have its stability status become more stable (see {@link Status}) at any time, including before the next
 * minor release. However, an API must not become less stable in the next minor release. Each stability status must
 * specify how API elements with that status may become less stable (e.g., with the next minor release, with next major
 * release).
 * </p>
 *
 * <p>
 * The key words "must", "must not", "require", "shall", "shall not", "should", "should not", and "may" in this document
 * are to be interpreted as described in RFC 2119.
 * </p>
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Retention(RetentionPolicy.CLASS)
@Documented
public @interface API {
    /**
     * Return the {@link Status} of the API element.
     * @return the current stability status of the annotated element
     */
    Status value();

    /**
     * An enum of possible API stability statuses. Each use of the {@code API} annotation must declare its status as
     * exactly one of these to indicate the stability of that API. The statuses shall be arranged in increasing order of
     * stability, so that a status shall be "less stable" than a status appearing after it in the enumeration.
     */
    enum Status {
        /**
         * Should not to be used by external code. This API is {@code public} only because it is needed by another
         * package within the Record Layer core. May change at any time, without prior notice, and without a change
         * in version. Shall not become less stable since this is the least stable status.
         */
        INTERNAL,

        /**
         * Deprecated code that should not be used in new code. Existing users should transition away from this API.
         * May be removed in the next minor release but must not be removed until the next minor release. May become
         * less stable in the next minor release.
         */
        DEPRECATED,

        /**
         * Used for new features under development where the API has not yet stabilized. May be used by external
         * consumers with caution, since it may change or be removed without notice or a change in version. May become
         * less stable in the next minor release.
         */
        EXPERIMENTAL,

        /**
         * Used by APIs that may change in the next minor release without prior notice. In contrast to those marked
         * {@code EXPERIMENTAL}, this API shall not be changed or be removed until the next minor release.
         */
        UNSTABLE,

        /**
         * Used for APIs that shall not be changed in a backwards-incompatible way or removed until the next major release.
         * May become less stable in the next major release.
         */
        STABLE
    }
}
