/*
 * LuceneIndexExpressions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The root expression of a {@code LUCENE} index specifies how select fields of a record are mapped to fields of a Lucene document.
 *
 * <p>
 * The expression tree is made up of the following.<br>
 * <b>Structure</b><ul>
 * <li>{@link ThenKeyExpression concat} includes multiple subexpressions in the index.
 * Since these are flattened, order does not really matter.</li>
 * <li>{@link NestingKeyExpression nest} traverses a nested subrecord.
 * By default, the name of the parent field is prepended to the names of descendent fields.</li>
 * </ul>
 *
 * <p>
 * <b>Fields</b><ul>
 * <li>{@link FieldKeyExpression field} is a record field whose value is added to the index.
 * By default, a field is indexed as a scalar value. That is, even a string with whitespace is a single token.
 * </li>
 * <li>{@link LuceneFunctionNames#LUCENE_TEXT function(lucene_text)} annotates a document field as text so that it is tokenized in the Lucene index.</li>
 * <li>{@link LuceneFunctionNames#LUCENE_STORED function(lucene_stored)} annotates a document field as additionally stored in the document so that its value is returned in searches.</li>
 * </ul>
 *
 * <p>
 * <b>Names</b><br>
 * By default, the name of each field in the hierarchy of nested subrecords is included in the name of flattened fields.
 * {@link LuceneFunctionNames#LUCENE_FIELD_NAME function(lucene_field_name)} overrides this.<ul>
 * <li>{@code value(null)} skips adding any name prefix, introducing the possibility of flattened name collisions.</li>
 * <li>{@code field(key)} allows another field to give the name. This is useful for map-like nested subrecords with well-known keys.</li>
 * </ul>
 *
 * <p>
 * The expression tree can be walked in several different ways, either with an actual record to produce actual fields, or with
 * record meta-data to determine what possible fields there are. Specifically,<ul>
 * <li>map a record into a document</li>
 * <li>get a list of document field names</li>
 * <li>validate the index expression at definition time</li>
 * <li>compute correlated matching expressions for the query planner</li>
 * </ul>
 */
public class LuceneIndexExpressions {
    private LuceneIndexExpressions() {
    }

    /**
     * Possible types for document fields.
     */
    public enum DocumentFieldType { STRING, TEXT, INT, LONG, DOUBLE, BOOLEAN }

    /**
     * Validate this key expression by interpreting it against the given meta-data.
     * @param root the {@code LUCENE} index root expresison
     * @param recordType Protobuf meta-data for record type
     */
    public static void validate(@Nonnull KeyExpression root, @Nonnull Descriptors.Descriptor recordType) {
        getFields(root, new MetaDataSource(recordType), (source, fieldName, value, type, stored, overriddeKeyRanges, groupingKeyIndex) -> {
        }, null);
    }

    /**
     * Information about how a document field is derived from a record field.
     */
    // TODO: Make this a JDK 14 record.
    public static class DocumentFieldDerivation {
        @Nonnull
        private final String documentField;
        @Nonnull
        private final List<String> recordFieldPath;
        @Nonnull
        private final DocumentFieldType type;
        private final boolean stored;

        public DocumentFieldDerivation(@Nonnull final String documentField, @Nonnull final List<String> recordFieldPath, @Nonnull final DocumentFieldType type, final boolean stored) {
            this.documentField = documentField;
            this.recordFieldPath = recordFieldPath;
            this.type = type;
            this.stored = stored;
        }

        @Nonnull
        public String getDocumentField() {
            return documentField;
        }

        @Nonnull
        public List<String> getRecordFieldPath() {
            return recordFieldPath;
        }

        @Nonnull
        public DocumentFieldType getType() {
            return type;
        }

        public boolean isStored() {
            return stored;
        }
    }

    /**
     * Get the derivations of known document fields.
     * @param root the {@code LUCENE} index root expresison
     * @param recordType Protobuf meta-data for record type
     * @return a map of document field names to {@link DocumentFieldDerivation}
     */
    public static Map<String, DocumentFieldDerivation> getDocumentFieldDerivations(@Nonnull KeyExpression root, @Nonnull Descriptors.Descriptor recordType) {
        final Map<String, DocumentFieldDerivation> fields = new HashMap<>();
        getFields(root,
                new MetaDataSource(recordType),
                (source, fieldName, value, type, stored, overriddenKeyRanges, groupingKeyIndex) -> {
                    List<String> path = new ArrayList<>();
                    for (MetaDataSource metaDataSource = source; metaDataSource != null; metaDataSource = metaDataSource.getParent()) {
                        if (metaDataSource.getField() != null) {
                            path.add(0, metaDataSource.getField());
                        }
                    }
                    path.add((String)value);
                    DocumentFieldDerivation derivation = new DocumentFieldDerivation(fieldName, path, type, stored);
                    fields.put(fieldName, derivation);
                }, null);
        return fields;
    }

    /**
     * An actual record / record meta-data.
     * @param <T> the actual type of this source
     */
    public interface RecordSource<T extends RecordSource<T>> {
        @Nonnull
        Descriptors.Descriptor getDescriptor();

        @Nonnull
        Iterable<T> getChildren(@Nonnull FieldKeyExpression parentExpression);

        @Nonnull
        Iterable<Object> getValues(@Nonnull FieldKeyExpression fieldExpression);
    }

    /**
     * An actual document / document meta-data.
     * @param <T> the actual type of the source
     */
    public interface DocumentDestination<T extends RecordSource<T>> {
        void addField(@Nonnull T source, @Nonnull String fieldName, @Nullable Object value, @Nonnull DocumentFieldType type,
                      boolean stored, @Nonnull List<Integer> overriddenKeyRanges, int groupingKeyIndex);
    }

    /**
     * Interpret the index key expression, either concretely for an actual record, or symbolically using meta-data.
     * @param root the {@code LUCENE} index root expresison
     * @param source the record / record meta-data
     * @param destination the document / document meta-data
     */
    @Nonnull
    public static <T extends RecordSource<T>> void getFields(@Nonnull KeyExpression root, @Nonnull T source,
                                                             @Nonnull DocumentDestination<T> destination, @Nullable String fieldNamePrefix) {
        KeyExpression expression;
        if (root instanceof GroupingKeyExpression) {
            expression = ((GroupingKeyExpression)root).getGroupedSubKey();
        } else {
            expression = root;
        }
        getFieldsRecursively(expression, source, destination, fieldNamePrefix, 0,
                root instanceof GroupingKeyExpression ? ((GroupingKeyExpression) root).getGroupingCount() : 0, new ArrayList<>());
    }

    @SuppressWarnings("squid:S3776")
    public static <T extends RecordSource<T>> void getFieldsRecursively(@Nonnull KeyExpression expression,
                                                                        @Nonnull T source, @Nonnull DocumentDestination<T> destination,
                                                                        @Nullable String fieldNamePrefix, int keyIndex, int groupingCount,
                                                                        @Nonnull List<Integer> overriddenKeyRanges) {
        if (expression instanceof ThenKeyExpression) {
            int count = 0;
            for (KeyExpression child : ((ThenKeyExpression)expression).getChildren()) {
                getFieldsRecursively(child, source, destination, fieldNamePrefix, keyIndex + count, groupingCount, overriddenKeyRanges);
                count += child.getColumnSize();
            }
            return;
        }

        String fieldNameSuffix = null;
        boolean suffixOverride = false;
        if (expression instanceof LuceneFunctionKeyExpression.LuceneFieldName) {
            LuceneFunctionKeyExpression.LuceneFieldName fieldNameExpression = (LuceneFunctionKeyExpression.LuceneFieldName)expression;
            KeyExpression nameExpression = fieldNameExpression.getNameExpression();
            if (nameExpression instanceof LiteralKeyExpression) {
                fieldNameSuffix = (String)((LiteralKeyExpression<?>)nameExpression).getValue();
            } else if (nameExpression instanceof FieldKeyExpression) {
                Iterator<Object> names = source.getValues((FieldKeyExpression)nameExpression).iterator();
                if (names.hasNext()) {
                    fieldNameSuffix = (String)names.next();
                    if (names.hasNext()) {
                        throw new RecordCoreException("Lucene field name override should evaluate to single value");
                    }
                }
            } else {
                throw new RecordCoreException("Lucene field name override should be a literal or a field");
            }
            suffixOverride = true;
            expression = fieldNameExpression.getNamedExpression();
        }

        if (expression instanceof NestingKeyExpression) {
            NestingKeyExpression nestingExpression = (NestingKeyExpression)expression;
            FieldKeyExpression parentExpression = nestingExpression.getParent();
            KeyExpression child = nestingExpression.getChild();
            if (!suffixOverride) {
                fieldNameSuffix = parentExpression.getFieldName();
            } else {
                addOverriddenKeyRange(overriddenKeyRanges, fieldNamePrefix, fieldNameSuffix);
            }
            String fieldName = appendFieldName(fieldNamePrefix, fieldNameSuffix);
            for (T subsource : source.getChildren(parentExpression)) {
                getFieldsRecursively(child, subsource, destination, fieldName, keyIndex, groupingCount, overriddenKeyRanges);
            }
            if (suffixOverride) {
                // Remove the last 2 numbers added above
                removedLastOverriddenKeyRange(overriddenKeyRanges);
            }
            return;
        }

        boolean fieldStored = false;
        boolean fieldText = false;
        while (true) {
            if (expression instanceof LuceneFunctionKeyExpression.LuceneStored) {
                LuceneFunctionKeyExpression.LuceneStored storedExpression = (LuceneFunctionKeyExpression.LuceneStored)expression;
                fieldStored = true;
                expression = storedExpression.getStoredExpression();
            } else if (expression instanceof LuceneFunctionKeyExpression.LuceneText) {
                LuceneFunctionKeyExpression.LuceneText textExpression = (LuceneFunctionKeyExpression.LuceneText)expression;
                fieldText = true;
                expression = textExpression.getFieldExpression();
            } else {
                // TODO: More text options.
                break;
            }
        }

        if (expression instanceof FieldKeyExpression) {
            FieldKeyExpression fieldExpression = (FieldKeyExpression)expression;
            if (!suffixOverride) {
                fieldNameSuffix = fieldExpression.getFieldName();
            } else {
                addOverriddenKeyRange(overriddenKeyRanges, fieldNamePrefix, fieldNameSuffix);
            }
            String fieldName = appendFieldName(fieldNamePrefix, fieldNameSuffix);
            if (fieldName == null) {
                fieldName = "_";
            }
            Descriptors.Descriptor recordDescriptor = source.getDescriptor();
            Descriptors.FieldDescriptor fieldDescriptor = recordDescriptor.findFieldByName(fieldExpression.getFieldName());
            DocumentFieldType fieldType;
            if (fieldText) {
                switch (fieldDescriptor.getJavaType()) {
                    case STRING:
                        fieldType = DocumentFieldType.TEXT;
                        break;
                    default:
                        throw new RecordCoreException("Unknown Lucene text field type");
                }
            } else {
                switch (fieldDescriptor.getJavaType()) {
                    case STRING:
                        fieldType = DocumentFieldType.STRING;
                        break;
                    case INT:
                        fieldType = DocumentFieldType.INT;
                        break;
                    case LONG:
                        fieldType = DocumentFieldType.LONG;
                        break;
                    case DOUBLE:
                        fieldType = DocumentFieldType.DOUBLE;
                        break;
                    case BOOLEAN:
                        fieldType = DocumentFieldType.BOOLEAN;
                        break;
                    default:
                        throw new RecordCoreException("Unknown Lucene field type");
                }
            }
            for (Object value : source.getValues(fieldExpression)) {
                destination.addField(source, fieldName, value, fieldType, fieldStored,
                        overriddenKeyRanges, keyIndex < groupingCount ? keyIndex : -1);
            }
            if (suffixOverride) {
                // Remove the last 2 numbers added above
                removedLastOverriddenKeyRange(overriddenKeyRanges);
            }
            return;
        }

        throw new RecordCoreException("Unknown Lucene field key expression");
    }

    private static void addOverriddenKeyRange(@Nonnull List<Integer> overriddenKeyRanges, @Nullable String fieldNamePrefix, @Nullable String fieldNameSuffix) {
        overriddenKeyRanges.add(fieldNamePrefix == null
                                ? 0
                                : fieldNamePrefix.length() + 1);
        overriddenKeyRanges.add(fieldNamePrefix == null
                                ? ((fieldNameSuffix == null || fieldNameSuffix.isEmpty()) ? 0 : fieldNameSuffix.length())
                                : (fieldNameSuffix == null || fieldNameSuffix.isEmpty()) ? fieldNamePrefix.length() + 1 : fieldNamePrefix.length() + fieldNameSuffix.length() + 1);
    }

    private static void removedLastOverriddenKeyRange(@Nonnull List<Integer> overriddenKeyRanges) {
        if (overriddenKeyRanges.size() < 2) {
            throw new RecordCoreException("Invalid call to remove last overridden key range, since the list has not a full range to remove");
        }
        overriddenKeyRanges.remove(overriddenKeyRanges.size() - 1);
        overriddenKeyRanges.remove(overriddenKeyRanges.size() - 1);
    }

    @Nullable
    private static String appendFieldName(@Nullable String fieldNamePrefix, @Nullable String fieldNameSuffix) {
        if (fieldNamePrefix == null) {
            return fieldNameSuffix;
        } else if (fieldNameSuffix == null) {
            return fieldNamePrefix;
        } else {
            return fieldNamePrefix + "_" + fieldNameSuffix;
        }
    }

    static class MetaDataSource implements RecordSource<MetaDataSource> {
        @Nullable
        private final MetaDataSource parent;
        @Nullable
        private final String field;
        @Nonnull
        private final Descriptors.Descriptor descriptor;

        MetaDataSource(@Nonnull Descriptors.Descriptor descriptor) {
            this(null, null, descriptor);
        }

        MetaDataSource(@Nullable MetaDataSource parent, @Nullable String field, @Nonnull Descriptors.Descriptor descriptor) {
            this.parent = parent;
            this.field = field;
            this.descriptor = descriptor;
        }

        @Nullable
        public MetaDataSource getParent() {
            return parent;
        }

        @Nullable
        public String getField() {
            return field;
        }

        @Override
        public Descriptors.Descriptor getDescriptor() {
            return descriptor;
        }

        @Override
        public Iterable<MetaDataSource> getChildren(@Nonnull FieldKeyExpression parentExpression) {
            final String parentField = parentExpression.getFieldName();
            final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(parentField);
            return Collections.singletonList(new MetaDataSource(this, parentField, fieldDescriptor.getMessageType()));
        }

        @Override
        public Iterable<Object> getValues(@Nonnull FieldKeyExpression fieldExpression) {
            return Collections.singletonList(fieldExpression.getFieldName());
        }
    }

}
