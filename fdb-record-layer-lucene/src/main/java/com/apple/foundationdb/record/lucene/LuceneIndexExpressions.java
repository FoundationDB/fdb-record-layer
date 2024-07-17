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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.search.BooleanPointsConfig;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.LiteralKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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
        getFields(root, new MetaDataSource(recordType), (source, fieldName, value, type, fieldNameOverride, namedFieldPath, namedFieldSuffix, stored, sorted, overriddeKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
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
        // TRUE when we have overridden the field name and replaced the original field with another
        private final boolean fieldNameOverride;
        // The replaced field name suffix (this has replaced the original in the documentField)
        @Nullable
        private final String namedFieldSuffix;
        private final boolean stored;
        private final boolean sorted;

        public DocumentFieldDerivation(@Nonnull String documentField, @Nonnull List<String> recordFieldPath,
                                       @Nonnull DocumentFieldType type,
                                       boolean fieldNameOverride, @Nullable String namedFieldSuffix,
                                       boolean stored, boolean sorted) {
            this.documentField = documentField;
            this.recordFieldPath = recordFieldPath;
            this.type = type;
            this.fieldNameOverride = fieldNameOverride;
            this.namedFieldSuffix = namedFieldSuffix;
            this.stored = stored;
            this.sorted = sorted;
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

        public boolean isFieldNameOverride() {
            return fieldNameOverride;
        }

        @Nullable
        public String getNamedFieldSuffix() {
            return namedFieldSuffix;
        }

        public boolean isStored() {
            return stored;
        }

        public boolean isSorted() {
            return sorted;
        }

        /**
         * Get the Point configuration that is used by the Lucene Parser to interpret the type of data
         * stored for this field.
         *
         * @return the PointsConfig for this field.
         */
        public PointsConfig getPointsConfig() {
            switch (type) {
                case INT:
                    return new PointsConfig(NumberFormat.getInstance(Locale.ROOT), Integer.class);
                case LONG:
                    return new PointsConfig(NumberFormat.getInstance(Locale.ROOT), Long.class);
                case DOUBLE:
                    return new PointsConfig(NumberFormat.getInstance(Locale.ROOT), Double.class);
                case BOOLEAN:
                    //booleans are weird, they are put in here to allow us to catch boolean fields in the parser
                    return new BooleanPointsConfig(NumberFormat.getInstance(Locale.ROOT));
                case STRING:
                case TEXT:
                default:
                    //we skip fields that are non-numeric because they don't parse differently in lucene anyway
                    return null;
            }
        }
    }

    @Nonnull
    public static Map<String, DocumentFieldDerivation> getDocumentFieldDerivations(@Nonnull final Index index, @Nonnull final RecordMetaData metadata) {
        Map<String, LuceneIndexExpressions.DocumentFieldDerivation> combined = null;
        for (RecordType recordType : metadata.recordTypesForIndex(index)) {
            final var documentFields = getDocumentFieldDerivations(index.getRootExpression(), recordType.getDescriptor());
            if (combined == null) {
                combined = documentFields;
            } else {
                combined.putAll(documentFields);
            }
        }
        if (combined == null) {
            combined = Collections.emptyMap();
        }
        return combined;
    }

    /**
     * Get the derivations of known document fields.
     * @param root the {@code LUCENE} index root expression
     * @param recordType Protobuf meta-data for record type
     * @return a map of document field names to {@link DocumentFieldDerivation}
     */
    public static Map<String, DocumentFieldDerivation> getDocumentFieldDerivations(@Nonnull KeyExpression root, @Nonnull Descriptors.Descriptor recordType) {
        final Map<String, DocumentFieldDerivation> fields = new HashMap<>();
        getFields(root,
                new MetaDataSource(recordType),
                (source, fieldName, value, type, fieldNameOverride, namedFieldPath, namedFieldSuffix, stored, sorted, overriddenKeyRanges, groupingKeyIndex, keyIndex, fieldConfigsIgnored) -> {
                    List<String> path = new ArrayList<>();
                    for (MetaDataSource metaDataSource = source; metaDataSource != null; metaDataSource = metaDataSource.getParent()) {
                        if (metaDataSource.getField() != null) {
                            path.add(0, metaDataSource.getField());
                        }
                    }
                    // Use the override path in case we are overriding the field name
                    if (fieldNameOverride && (namedFieldPath != null)) {
                        path.addAll(namedFieldPath);
                    } else {
                        path.add((String)value);
                    }

                    DocumentFieldDerivation derivation = new DocumentFieldDerivation(fieldName, path, type, fieldNameOverride, namedFieldSuffix, stored, sorted);
                    fields.put(fieldName, derivation);
                }, null);
        return fields;
    }

    /**
     * Constructs a point-config map for a given {@link Index} fields.
     * @param store The record store, used to retrieve metadata.
     * @param index The index.
     * @return a mapping between an index field name and its {@link PointsConfig}.
     */
    @Nonnull
    public static Map<String, PointsConfig> constructPointConfigMap(@Nonnull final FDBRecordStoreBase<?> store,
                                                                    @Nonnull Index index) {
        final Map<String, PointsConfig> result = new HashMap<>();
        for (final RecordType type : store.getRecordMetaData().recordTypesForIndex(index)) {
            LuceneIndexExpressions.getDocumentFieldDerivations(index.getRootExpression(), type.getDescriptor()).forEach((key, value) -> {
                final PointsConfig valueConfig = value.getPointsConfig();
                if (valueConfig != null) {
                    final PointsConfig oldConfig = result.get(key);
                    if (oldConfig == null) {
                        result.put(key, valueConfig);
                    } else if (!oldConfig.equals(valueConfig)) {
                        throw new RecordCoreException("The same key has two different points config types").addLogInfo(LogMessageKeys.KEY, key);
                    }
                }
            });
        }
        return result;
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
        Iterable<Object> getValues(@Nonnull KeyExpression keyExpression);
    }

    /**
     * An actual document / document meta-data.
     * @param <T> the actual type of the source
     */
    public interface DocumentDestination<T extends RecordSource<T>> {
        /**
         * Add fields to the destination of the "getFields" traversal.
         * @param source the RecordSource for the field to be added
         * @param fieldName the (full) field name
         * @param value the last element of the field name
         * @param type the type of the field
         * @param fieldNameOverride whether we are overriding field names (map support)
         * @param namedFieldPath the relative path to the field (from the source)
         * @param namedFieldSuffix the replaced suffix for the field (the map-replaced part)
         * @param stored whether the field has stored data
         * @param sorted whether the field has sorted data
         * @param overriddenKeyRanges -
         * @param groupingKeyIndex -
         * @param keyIndex -
         * @param fieldConfigs -
         */
        @SuppressWarnings("java:S107")
        void addField(@Nonnull T source, @Nonnull String fieldName, @Nullable Object value, @Nonnull DocumentFieldType type,
                      boolean fieldNameOverride, @Nullable List<String> namedFieldPath, @Nullable String namedFieldSuffix,
                      boolean stored, boolean sorted, @Nonnull List<Integer> overriddenKeyRanges, int groupingKeyIndex, int keyIndex, @Nonnull Map<String, Object> fieldConfigs);
    }

    /**
     * Interpret the index key expression, either concretely for an actual record, or symbolically using meta-data.
     * @param <T> specific kind of {@link RecordSource}
     * @param root the {@code LUCENE} index root expresison
     * @param source the record / record meta-data
     * @param destination the document / document meta-data
     * @param fieldNamePrefix prefix for generated field names
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
        // Record type evaluation of primary key based on this expression for the partial record is not needed,
        // because this partial record to build has the correct record type.
        if (expression instanceof RecordTypeKeyExpression) {
            return;
        }

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
        // Non null when using LuceneFieldName, will then contain the relative path to the field from the name expression (the replacement name)
        List<String> namedFieldPath = null;
        if (expression instanceof LuceneFunctionKeyExpression.LuceneFieldName) {
            LuceneFunctionKeyExpression.LuceneFieldName fieldNameExpression = (LuceneFunctionKeyExpression.LuceneFieldName)expression;
            KeyExpression nameExpression = fieldNameExpression.getNameExpression();
            if (nameExpression instanceof LiteralKeyExpression) {
                fieldNameSuffix = (String)((LiteralKeyExpression<?>)nameExpression).getValue();
            } else if (nameExpression instanceof FieldKeyExpression) {
                Iterator<Object> names = source.getValues(nameExpression).iterator();
                if (names.hasNext()) {
                    fieldNameSuffix = (String)names.next();
                    if (names.hasNext()) {
                        throw new RecordCoreException("Lucene field name override should evaluate to single value");
                    }
                    // Use the only name in the expression for the path
                    namedFieldPath = Collections.singletonList(fieldNameSuffix);
                }
            } else if (nameExpression instanceof NestingKeyExpression) {
                // Get the nameField suffix and path to be used in replacing the namedField and path
                final Iterable<Object> values = source.getValues(nameExpression);
                namedFieldPath = Streams.stream(values).map(String.class::cast).collect(Collectors.toList());
                fieldNameSuffix = String.join("_", namedFieldPath);
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

        boolean fieldSorted = false;
        boolean fieldStored = false;
        boolean fieldText = false;
        Map<String, Object> configs = Collections.emptyMap();
        while (true) {
            if (expression instanceof LuceneFunctionKeyExpression.LuceneSorted) {
                LuceneFunctionKeyExpression.LuceneSorted sortedExpression = (LuceneFunctionKeyExpression.LuceneSorted)expression;
                fieldSorted = true;
                expression = sortedExpression.getSortedExpression();
            } else if (expression instanceof LuceneFunctionKeyExpression.LuceneStored) {
                LuceneFunctionKeyExpression.LuceneStored storedExpression = (LuceneFunctionKeyExpression.LuceneStored)expression;
                fieldStored = true;
                expression = storedExpression.getStoredExpression();
            } else if (expression instanceof LuceneFunctionKeyExpression.LuceneText) {
                LuceneFunctionKeyExpression.LuceneText textExpression = (LuceneFunctionKeyExpression.LuceneText)expression;
                fieldText = true;
                expression = textExpression.getFieldExpression();
                configs = textExpression.getFieldConfigs();
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
                destination.addField(source, fieldName, value, fieldType, suffixOverride, namedFieldPath, fieldNameSuffix, fieldStored, fieldSorted,
                        overriddenKeyRanges, keyIndex < groupingCount ? keyIndex : -1, keyIndex, configs);
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
        if (fieldNamePrefix == null) {
            overriddenKeyRanges.add(0);
            overriddenKeyRanges.add((fieldNameSuffix == null || fieldNameSuffix.isEmpty()) ? 0 : fieldNameSuffix.length());
        } else {
            overriddenKeyRanges.add(fieldNamePrefix.length() + 1);
            overriddenKeyRanges.add((fieldNameSuffix == null || fieldNameSuffix.isEmpty()) ? fieldNamePrefix.length() + 1 : fieldNamePrefix.length() + fieldNameSuffix.length() + 1);
        }
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
        public Iterable<Object> getValues(@Nonnull KeyExpression keyExpression) {
            List<Object> result = new ArrayList<>();
            KeyExpression current = keyExpression;
            while (current != null) {
                if (current instanceof NestingKeyExpression) {
                    NestingKeyExpression expression = (NestingKeyExpression)current;
                    result.add(expression.getParent().getFieldName());
                    current = expression.getChild();
                } else if (current instanceof FieldKeyExpression) {
                    result.add(((FieldKeyExpression)current).getFieldName());
                    current = null;
                } else {
                    throw new RecordCoreArgumentException("Nested key type not supported for values")
                            .addLogInfo("keyType", current.getClass().getName());
                }
            }
            return result;
        }
    }
}
