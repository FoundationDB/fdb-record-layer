/*
 * KeySpacePathParsingTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class KeySpacePathParsingTest {
    private final KeySpace testSpace = getKeySpaceForTesting();
    @Test
    void testParsingKeySpacePath() {
        final List<Object> url = new ArrayList<>();
        url.add(null);
        url.add("prod");
        url.add("testApp");
        url.add(12345L);

        URI expected = URI.create("/prod/testApp/12345");
        KeySpacePath path = KeySpaceUtils.uriToPath(expected,testSpace);
        final URI uri = KeySpaceUtils.pathToURI(path);
        Assertions.assertEquals(expected,uri,"Invalid parsing of URI or KeySpacePaths");
    }

    @Test
    void cannotParseEmptyUri() {
        RelationalException ve = Assertions.assertThrows(RelationalException.class,()->KeySpaceUtils.uriToPath(URI.create(""),testSpace));
        Assertions.assertEquals(RelationalException.ErrorCode.INVALID_PATH,ve.getErrorCode(),"Incorrect returned error code");
    }

    @Test
    void testUrlNotValidForKeySpace() {
        //throws the right exception when we can't parse an entry
        RelationalException ve = Assertions.assertThrows(RelationalException.class,()->KeySpaceUtils.uriToPath(URI.create("/prod/testApp/notAUser"),testSpace));
        Assertions.assertEquals(RelationalException.ErrorCode.INVALID_PATH,ve.getErrorCode(),"Incorrect returned error code");
    }

    private KeySpace getKeySpaceForTesting() {
        final KeySpaceDirectory env = new KeySpaceDirectory("Environment", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory app = new KeySpaceDirectory("App", KeySpaceDirectory.KeyType.STRING);
        final KeySpaceDirectory user = new KeySpaceDirectory("User", KeySpaceDirectory.KeyType.LONG);
        env.addSubdirectory(app);
        app.addSubdirectory(user);
        return new KeySpace(env);
    }
}
