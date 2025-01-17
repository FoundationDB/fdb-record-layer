/*
 * JDBCURI.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * JDBC connection URI public utility and constants.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("AbbreviationAsWordInName") // should consider revising
public final class JDBCURI {
    public static final String JDBC_URL_PREFIX = "jdbc:";
    public static final String JDBC_URL_SCHEME = "relational";

    /**
     * Base URL.
     * {@link JDBCRelationalDriver#acceptsURL(String)} accepts any jdbc url that starts with the below.
     * Used to check passed urls in {@link JDBCRelationalDriver#acceptsURL(String)} but also as a key
     * to pick out the loaded driver from DriverManager.
     */
    public static final String JDBC_BASE_URL = JDBC_URL_PREFIX + JDBC_URL_SCHEME + "://";

    /**
     * Key we use to get servername parameter from query component of an 'inprocess' JDBC URI.
     * For example, jdbc:relational:///__SYS?server=123e4567-e89b-12d3-a456-426614174000.
     */
    public static final String INPROCESS_URI_QUERY_SERVERNAME_KEY = "server";

    private JDBCURI() {
    }

    /**
     * Return first value in the values list.
     * @param key Key to use looking up values list.
     * @param map Map to search.
     * @return First value in values list or null if no values found.
     */
    public static String getFirstValue(String key, Map<String, List<String>> map) {
        List<String> values = map.get(key);
        return values == null ? null : values.get(0);
    }

    /**
     * From https://stackoverflow.com/questions/13592236/parse-a-uri-string-into-name-value-collection.
     * Converts UnsupportedEncodingException to a RuntimeException should it ever occur.
     * @param  uri JDBC Connect URL.
     * @return Map of lists of query string parameters keyed by query parameter name.
     */
    @Nonnull
    public static Map<String, List<String>> splitQuery(URI uri) {
        final Map<String, List<String>> params = new LinkedHashMap<>();
        if (uri.getQuery() == null || uri.getQuery().length() <= 0) {
            return params;
        }
        final String[] pairs =  uri.getQuery().split("&");
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? urlDecoder(pair.substring(0, idx)) : pair;
            if (!params.containsKey(key)) {
                params.put(key, new LinkedList<>());
            }
            final String value = idx > 0 && pair.length() > idx + 1 ?
                    urlDecoder(pair.substring(idx + 1)) : null;
            params.get(key).add(value);
        }
        return params;
    }

    /**
     * Run UTF-8 URLDecoding of <code>s</code> and if exception, convert to RuntimeException so we don't have to
     * catch decoding exception {@link UnsupportedEncodingException} all up the stack, especially when it should never
     * happen.
     * @param s String to URLDecode.
     * @return URL decoded String.
     */
    private static String urlDecoder(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI addQueryParameter(URI uri, String key, String value) {
        Map<String, List<String>> queryParams = splitQuery(uri);
        if (queryParams.get(key) != null) {
            // Expectation is that the key does not exist. Fail if it does.
            throw new UnsupportedOperationException("Can not overwrite existing " + key + " in " + uri);
        }
        return URI.create(uri.toString() + "&" + key + "=" + value);
    }
}
