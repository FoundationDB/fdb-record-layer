/*
 * BuildVersionTest.java
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

package com.apple.foundationdb.relational.util;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

@Disabled("update for new version scheme")
public class BuildVersionTest {
    @Test
    public void testGetURL() throws URISyntaxException {
        String url = BuildVersion.getInstance().getURL();
        Assertions.assertThat(url).contains("relational");
        // Assert parses as URI.
        new URI(url);
    }

    @Test
    public void getPieceOfDriverVersion() throws RelationalException {
        final String version = "2345B6";
        BuildVersion bv = BuildVersion.getInstance();
        Assertions.assertThat(23 == bv.parseDriverVersion(version, 0));
        Assertions.assertThat(45 == bv.parseDriverVersion(version, 1));
        Assertions.assertThat(6 == bv.parseDriverVersion(version, 2));
        Assertions.assertThatThrownBy(() -> bv.parseDriverVersion(version, 3))
                .isExactlyInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    public void testGetMinorVersion() throws RelationalException {
        BuildVersion bv = BuildVersion.getInstance();
        String version = bv.getInstance().getVersion();
        int minorVersion = bv.getInstance().getMinorVersion();
        int mv = Integer.parseInt(version.substring(2, 4));
        Assertions.assertThat(mv).isEqualTo(minorVersion);
    }

    @Test
    public void testGetMajorVersion() throws RelationalException {
        BuildVersion bv = BuildVersion.getInstance();
        String version = bv.getInstance().getVersion();
        int majorVersion = bv.getInstance().getMajorVersion();
        int mv = Integer.parseInt(version.substring(0, 2));
        Assertions.assertThat(mv).isEqualTo(majorVersion);
    }
}
