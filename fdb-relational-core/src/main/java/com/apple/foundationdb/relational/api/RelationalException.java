/*
 * RelationalException.java
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

package com.apple.foundationdb.relational.api;

public class RelationalException extends RuntimeException{
    private final ErrorCode errorCode;

    public RelationalException(String message,ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public RelationalException(String message,ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RelationalException(ErrorCode errorCode,Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public static RelationalException convert(Throwable re) {
        return new RelationalException(ErrorCode.UNKNOWN,re);
    }

    public ErrorCode getErrorCode(){
        return errorCode;
    }

    //TODO(bfines) add proper error code patterns --these patterns are currently randomly selected
    public enum ErrorCode{
        INVALID_TYPE("U001"),
        UNIQUE_CONSTRAINT_VIOLATION("UC0V"),
        TRANSACTION_INACTIVE("TX0IN"),
        NO_SUCH_SCHEMA("U0NSS"),
        UNKNOWN_DATABASE("U0UKD"),
        UNKNOWN_SCHEMA("U0UKS"),
        UNKNOWN_TABLE("U0UKT"),
        QUERY_TIMEOUT("S0QT"),
        UNKNOWN("UK000"),
        UNKNOWN_INDEX("TB00UI"),
        NO_SUCH_FIELD("RSNSF"), //no field of specified name in result set
        CANNOT_CONVERT_TO_MESSAGE("RSCTM"), //cannot convert a ResultRow to a Message object
        INVALID_PATH("U0IP1"),
        INVALID_PRIMARYKEY_SET("U0IPKS");

        private final String errorCode;

        ErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        String getCodeString(){
            return errorCode;
        }
    }
}
