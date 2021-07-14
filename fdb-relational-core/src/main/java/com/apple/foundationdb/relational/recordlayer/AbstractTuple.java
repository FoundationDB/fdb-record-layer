/*
 * AbstractTuple.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.InvalidTypeException;
import com.apple.foundationdb.relational.api.NestableTuple;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class AbstractTuple implements NestableTuple {

    @Override
    public long getLong(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }
        Object o = getObject(position);
        if(!(o instanceof Number)){
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a scalar type");
        }
        return ((Number)o).longValue();
    }

    @Override
    public float getFloat(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);
        if(!(o instanceof Number)){
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a float type");
        }
        return ((Number)o).floatValue();
    }

    @Override
    public double getDouble(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);
        if(!(o instanceof Number)){
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a double type");
        }
        return ((Number)o).doubleValue();
    }

    @Override
    public String getString(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);
        if(!(o instanceof String)){
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a String");
        }
        return (String)o;
    }

    @Override
    public byte[] getBytes(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);
        if(!(o instanceof byte[])){
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a byte[]");
        }
        return (byte[])o;
    }

    @Override
    public NestableTuple getTuple(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);
        if(o instanceof NestableTuple){
            return (NestableTuple)o;
        }else if(o instanceof Tuple){
            return new FDBTuple((Tuple)o);
        }else{
            throw new InvalidTypeException("Value <"+o+"> cannot be cast to a tuple");
        }
    }

    @Override
    public Iterable<NestableTuple> getArray(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= getNumFields()){
            throw new IllegalArgumentException();
        }

        Object o = getObject(position);

        if (o instanceof Iterable) {
            return StreamSupport.stream(((Iterable<?>) o).spliterator(), false).map(obj -> {
                if (obj instanceof NestableTuple) {
                    return (NestableTuple) obj;
                } else if (obj instanceof Tuple) {
                    return new FDBTuple((Tuple) obj);
                } else {
                    return new ValueTuple(obj);
                }
            }).collect(Collectors.toList());
        } else {
            throw new InvalidTypeException("Object <" + o + "> cannot be converted to a repeated type");
        }
    }

}
