/*
 * BuildVersionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class BuildVersionTest {
    @Test
    public void testGetURL() throws URISyntaxException {
        String url = BuildVersion.getInstance().getURL();
        Assertions.assertThat(url).contains("relational");
        // Assert parses as URI.
        new URI(url);
    }

    @Test
    public void getPieceOfDriverVersion() {
        final String version = "1.2.3";
        BuildVersion bv = BuildVersion.getInstance();
        Assertions.assertThat(1 == bv.parseDriverVersion(version, 0));
        Assertions.assertThat(2 == bv.parseDriverVersion(version, 1));
        Assertions.assertThat(3 == bv.parseDriverVersion(version, 2));
        Assertions.assertThatThrownBy(() -> bv.parseDriverVersion(version, 3))
                .isExactlyInstanceOf(ArrayIndexOutOfBoundsException.class);
        String driverVersion = bv.getInstance().getVersion();
        int offset = assertParseDriverVersion(driverVersion, 0, 0);
        assertParseDriverVersion(driverVersion, offset, 1);
    }

    /**
     * Assert our parseDriverVersion is doing the right thing.
     * @return The offset at where to start looking for the next part... presumes we are moving
     * sequentially through the version string.
     */
    private int assertParseDriverVersion(String version, int offset, int versionPartIndex) {
        int index = version.indexOf(".", offset);
        // Get the 'major' part of the version.
        int i = Integer.parseInt(version.substring(offset, index));
        int part = BuildVersion.getInstance().parseDriverVersion(version, versionPartIndex);
        Assertions.assertThat(i).isEqualTo(part);
        return index + 1;
    }

    @Test
    public void testGetMinorVersion() {
        BuildVersion bv = BuildVersion.getInstance();
        String version = bv.getInstance().getVersion();
        int minorVersion = bv.getInstance().getMinorVersion();
        int index = version.indexOf(".");
        String subStr = version.substring(index + 1);
        // Our version strings are a bit odd -- they are 'dates' that can be have zero-padding -- and this
        // 'test' is a little silly/awkward checking that the minor part starts with the 'expected'
        // number... accommodate zero-padding.
        if (subStr.startsWith("0")) {
            subStr = subStr.substring(1);
        }
        Assertions.assertThat(subStr).startsWith(Integer.toString(minorVersion));
    }

    @Test
    public void testGetMajorVersion() {
        BuildVersion bv = BuildVersion.getInstance();
        String version = bv.getInstance().getVersion();
        int majorVersion = bv.getInstance().getMajorVersion();
        Assertions.assertThat(version).startsWith(Integer.toString(majorVersion));
    }

    @Test
    public void testGetGitHash() {
        Assertions.assertThat(BuildVersion.getInstance().getGitHash().length()).isEqualTo(40);
    }
}
