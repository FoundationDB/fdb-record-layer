/*
 * FDBTuple.java
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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

class FDBTuple extends AbstractTuple {
    private Tuple t;

    public FDBTuple(Tuple t) {
        this.t = t;
    }

    /**
     * Copy-constructor to make an FDB tuple from another type of tuple
     * @param copy the tuple to copy
     */
    FDBTuple(@Nonnull NestableTuple copy){
        this.t = new Tuple();
        for(int i=0;i<copy.getNumFields();i++){
            this.t.addObject(copy.getObject(i));
        }
    }

    void setTuple(Tuple t){
        this.t = t;
    }

    @Override
    public int getNumFields() {
        return t.size();
    }

    @Override
    public Object getObject(int position) {
        return t.get(position);
    }

    @Override
    public NestableTuple getTuple(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= t.size()){
            throw new IllegalArgumentException();
        }
        try {
            return new FDBTuple(t.getNestedTuple(position));
        }catch(ClassCastException cce){
            throw new InvalidTypeException("Object <"+t.get(position)+"> cannot be converted to a Tuple type");
        }
    }

    @Override
    public Iterable<NestableTuple> getArray(int position) throws InvalidTypeException, IllegalArgumentException {
        if(position <0 || position >= t.size()){
            throw new IllegalArgumentException();
        }
        try {
            final List<Object> nestedList = t.getNestedList(position);
            return nestedList.stream().map((obj) -> {
                if (obj instanceof Tuple) {
                    return new FDBTuple((Tuple) obj);
                } else {
                    return new ValueTuple(obj);
                }
            }).collect(Collectors.toList());
        }catch(ClassCastException cce){
            throw new InvalidTypeException("Object <"+t.get(position)+"> cannot be converted to an iterable type");
        }
    }

    Tuple fdbTuple() {
        return t;
    }
}
