/*
 * KeySpace.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A <code>KeySpace</code> defines a logical directory structure for keys that comprise an FDB row key.
 * Directories within a <code>KeySpace</code> are defined by the following attributes:
 * <ul>
 *     <li><b>name</b> - Defines a logical name for the directory. As with a directory in a filesystem, Within a given
 *         directory no two sub-directories may have the same name</li>
 *     <li><b>type</b> - Defines the datatype for the values stored at the directory's position within the FDB keyspace.
 *         No two directories may have the same type unless they are defined with different constant values.
 *         Note that <code>NULL</code> is considered a unique type, as such, all other types are implicitly non-null
 *         and any attempt to store a null value for those types will result in an error</li>
 *     <li><b>constant</b> - (optional) Defines a constant value for the directory within the FDB keyspace.</li>
 * </ul>
 *
 * <p>Put visually, the following directory structure:
 * <pre>
 *  / (NULL)
 *     +- state (STRING)
 *        +- office_id (LONG)
 *           +- employees (STRING=E)
 *           |  +- employee_id (LONG)
 *           +- inventory (STRING=I)
 *           |  +- stock_id (LONG)
 *           +- sales (STRING=S)
 *              +- transaction_id (UUID)
 *              +- layaways (NULL)
 *                 +- transaction_id (UUID)
 *
 * </pre>
 * defines a directory structure in which:
 * <ul>
 *     <li> A <code>state</code> is represented by a string (presumably a value like * "CA", "NJ', "NY", etc.).
 *     <li> Within a <code>state</code> there are offices, each represented by a unique <code>office_id</code>.
 *     <li> Within a given <code>office_id</code>, there are three categories of information:
 *       <ul>
 *          <li> A set of <code>employees</code> (contained within a directory represented by the string "E"). Each
 *              employee is keyed by a LONG <code>employee_id</code> value.</li>
 *          <li> The inventory of the office (contained within a directory represented by the string "I"). Each
 *              inventory item is keyed by a LONG <code>stock_id</code> value.</li>
 *          <li> A list of sales transactions (contained within a directory represented by the string "S")
 *              Each transaction is represented by a UUID <code>transaction_id</code> value.  There is a
 *              special location stored until the NULL key to indicate transactions that are layaways (haven't
 *              been fully paid for yet).</li>
 *       </ul>
 *     </li>
 * </ul>
 *
 * <p>
 * Such a directory structure would be constructed as:
 * <pre>
 *  KeySpace keySpace = new KeySpace(
 *          new KeySpaceDirectory("state", KeyType.STRING)
 *              .addSubdirectory(new KeySpaceDirectory("office_id", KeyType.LONG)
 *                  .addSubdirectory(new KeySpaceDirectory("employees", KeyType.STRING, "E")
 *                      .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)))
 *                  .addSubdirectory(new KeySpaceDirectory("inventory", KeyType.STRING, "I")
 *                      .addSubdirectory(new KeySpaceDirectory("stock_id", KeyType.LONG)))
 *                  .addSubdirectory(new KeySpaceDirectory("sales", KeyType.STRING, "S")
 *                      .addSubdirectory(new KeySpaceDirectory("transaction_id", KeyType.UUID))
 *                      .addSubdirectory(new KeySpaceDirectory( "layaways", KeyType.NULL)))));
 * </pre>
 *
 * <p>
 * Once defined, a <code>KeySpace</code> provides a convenient mechanism for constructing tuples that may then
 * be used to form an FDB row key.  For example:
 * <pre>
 *     Tuple transactionKey = keySpace.path("state", "CA")
 *         .add("office_id", 1234)
 *         .add("sales")
 *         .add("transaction_id", UUID.randomUUID())
 *         .toTuple(context);
 * </pre>
 * Creates a row key suitable to store a new sales transaction in California in office 1234.
 *
 * <p>Similarly, a <code>KeySpace</code> can be used to take a given FDB row key and logically turn it back into the
 * path from which it came:
 * <pre>
 *     KeySpacePath path = keySpace.pathFromKey(context, transactionKey);
 *     System.out.println(path.getDirectoryName());  //  Displays "transaction_id=XXXX-XXXX-XXXX-XXX""
 *     System.out.println(path.getParent().getDirectoryName());  //  Displays "sales"
 *     System.out.println(path.getParent().getParent().getDirectoryName());  //  Displays "office_id"
 * </pre>
 */
@API(API.Status.UNSTABLE)
public class KeySpace {

    private final KeySpaceDirectory root;

    public KeySpace(@Nonnull KeySpaceDirectory ... rootDirectories) {
        this("/", rootDirectories);
    }

    public KeySpace(@Nonnull String name, @Nonnull KeySpaceDirectory ... rootDirectories) {
        root = new KeySpaceDirectory(name, KeySpaceDirectory.KeyType.NULL, null, null);
        for (KeySpaceDirectory directory : rootDirectories) {
            root.addSubdirectory(directory);
        }
    }

    /**
     * Retrieves a subdirectory.
     * @param name the name of the directory to return
     * @return the directory with the specified <code>name</code>
     * @throws NoSuchDirectoryException if the directory does not exist
     */
    @Nonnull
    public KeySpaceDirectory getDirectory(@Nonnull String name) {
        return root.getSubdirectory(name);
    }

    /**
     * Returns the available root directories in the <code>KeySpace</code>.
     * @return a list of root directories
     */
    @Nonnull
    public List<KeySpaceDirectory> getDirectories() {
        return root.getSubdirectories();
    }

    /**
     * Begin a path traversal from a root directory. This method may only be used for root directories
     * defined with a constant value.
     *
     * @param name the name of the root directory with which to begin the path
     * @return the path beginning at the specified root directory
     * @throws NoSuchDirectoryException if the directory does not exist
     */
    @Nonnull
    public KeySpacePath path(@Nonnull String name) {
        KeySpaceDirectory dir = root.getSubdirectory(name);
        return KeySpacePathImpl.newPath(null, dir);
    }

    /**
     * Begin a path traversal from a root directory.
     *
     * @param name the name of the root directory with which to begin the path
     * @param value the value to use for the directory
     * @return the path beginning at the specified root directory
     * @throws NoSuchDirectoryException if the directory does not exist
     * @throws RecordCoreArgumentException if the value provided is incompatible with the data type declared
     *    for the directory
     */
    @Nonnull
    public KeySpacePath path(@Nonnull String name, @Nullable Object value) {
        KeySpaceDirectory dir = root.getSubdirectory(name);
        return KeySpacePathImpl.newPath(null, dir, value, false, null, null);
    }

    /**
     * Given a tuple from an FDB key, attempts to determine what path through this directory the tuple
     * represents, returning a <code>KeySpacePath</code> representing the leaf-most directory in the path.
     * If entries remained in the tuple beyond the leaf directory, then
     *   {@link KeySpacePath#getRemainder()} can be used to fetch the remaining portion.
     *
     * @param context context used, if needed, for any database operations
     * @param key the tuple to be decoded
     * @return a path entry representing the leaf directory entry that corresponds to a value in the
     * provided tuple
     * @throws RecordCoreArgumentException if the tuple provided does not correspond to any path through
     *   the directory structure at this point
     *
     * @deprecated use {@link #resolveFromKeyAsync(FDBRecordContext, Tuple)} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    @Nonnull
    public CompletableFuture<KeySpacePath> pathFromKeyAsync(@Nonnull FDBRecordContext context, @Nonnull Tuple key) {
        return root.findChildForKey(context, null, key, key.size(), 0).thenApply(ResolvedKeySpacePath::toPath);
    }

    /**
     * Synchronous/blocking version of <code>pathFromKeyAsync</code>.
     *
     * @param context context used, if needed, for any database operations
     * @param key the tuple to be decoded
     * @return a path entry representing the leaf directory entry that corresponds to a value in the
     * provided tuple
     * @throws RecordCoreArgumentException if the tuple provided does not correspond to any path through
     *   the directory structure at this point
     *
     * @deprecated use {@link #resolveFromKey(FDBRecordContext, Tuple)} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    @Nonnull
    public KeySpacePath pathFromKey(@Nonnull FDBRecordContext context, @Nonnull Tuple key) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, pathFromKeyAsync(context, key));
    }

    /**
     * Given a tuple from an FDB key, attempts to determine what path through this directory the tuple
     * represents, returning a <code>ResolvedKeySpacePath</code> representing the leaf-most directory in the path.
     * <p>
     *     If entries remained in the tuple beyond the leaf directory, then {@link KeySpacePath#getRemainder()} can be
     *     used to fetch the remaining portion.
     *     See also {@link KeySpacePath#toResolvedPathAsync(FDBRecordContext, byte[])} if you need to resolve and you
     *     know that it is part of a given path.
     * </p>
     * @param context context used, if needed, for any database operations
     * @param key the tuple to be decoded
     * @return a path entry representing the leaf directory entry that corresponds to a value in the
     * provided tuple
     * @throws RecordCoreArgumentException if the tuple provided does not correspond to any path through
     *   the directory structure at this point
     */
    @Nonnull
    public CompletableFuture<ResolvedKeySpacePath> resolveFromKeyAsync(@Nonnull FDBRecordContext context, @Nonnull Tuple key) {
        return root.findChildForKey(context, null, key, key.size(), 0);
    }

    /**
     * Synchronous/blocking version of <code>resolveFromKeyAsync</code>.
     *
     * @param context context used, if needed, for any database operations
     * @param key the tuple to be decoded
     * @return a path entry representing the leaf directory entry that corresponds to a value in the
     * provided tuple
     * @throws RecordCoreArgumentException if the tuple provided does not correspond to any path through
     *   the directory structure at this point
     */
    @Nonnull
    public ResolvedKeySpacePath resolveFromKey(@Nonnull FDBRecordContext context, @Nonnull Tuple key) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, resolveFromKeyAsync(context, key));
    }

    /**
     * List the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param subdirName the name of the subdirectory to list
     * @param continuation if non-null, provides a continuation from a previous listing
     * @param scanProperties the properties to be used to control how the scan is performed
     * @return a cursor over the paths that were found
     *
     * @deprecated use {@link #listDirectoryAsync(FDBRecordContext, String, byte[], ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    public RecordCursor<KeySpacePath> listAsync(@Nonnull FDBRecordContext context, @Nonnull String subdirName,
                                                @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return root.listSubdirectoryAsync(null, context, subdirName, continuation, scanProperties)
                .map(ResolvedKeySpacePath::toPath);
    }

    /**
     * Synchronously list the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param directory the path under which to list
     * @param scanProperties the properties to be used to control how the scan is performed
     * @return a list of the paths that were found
     *
     * @deprecated use {@link #listDirectoryAsync(FDBRecordContext, String, byte[], ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    public List<KeySpacePath> list(@Nonnull FDBRecordContext context, @Nonnull String directory,
                                   @Nonnull ScanProperties scanProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST,
                listAsync(context, directory, null, scanProperties).asList());
    }


    /**
     * Synchronously list the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param directory the path under which to list
     * @return a list of the paths that were found
     * @deprecated use {@link #listDirectory(FDBRecordContext, String)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    public List<KeySpacePath> list(@Nonnull FDBRecordContext context, @Nonnull String directory) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST,
                listAsync(context, directory, null, ScanProperties.FORWARD_SCAN).asList());
    }

    /**
     * List the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param directory the name of the directory to list
     * @param continuation if non-null, provides a continuation from a previous listing
     * @param scanProperties the properties to be used to control how the scan is performed
     * @return a cursor over the paths that were found
     */
    @Nonnull
    public RecordCursor<ResolvedKeySpacePath> listDirectoryAsync(@Nonnull FDBRecordContext context,
                                                                 @Nonnull String directory,
                                                                 @Nullable byte[] continuation,
                                                                 @Nonnull ScanProperties scanProperties) {
        return root.listSubdirectoryAsync(null, context, directory, continuation, scanProperties);
    }

    /**
     * Synchronously list the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param directory the path under which to list
     * @param continuation if non-null, provides a continuation from a previous listing
     * @param scanProperties the properties to be used to control how the scan is performed
     * @return a list of the paths that were found
     */
    @Nonnull
    public List<ResolvedKeySpacePath> listDirectory(@Nonnull FDBRecordContext context,
                                                    @Nonnull String directory,
                                                    @Nullable byte[] continuation,
                                                    @Nonnull ScanProperties scanProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST,
                listDirectoryAsync(context, directory, continuation, scanProperties).asList());
    }

    /**
     * Synchronously list the available paths from a directory.
     *
     * @param context the context in which to perform database access
     * @param directory the path under which to list
     * @param scanProperties the properties to be used to control how the scan is performed
     * @return a list of the paths that were found
     */
    @Nonnull
    public List<ResolvedKeySpacePath> listDirectory(@Nonnull FDBRecordContext context,
                                                    @Nonnull String directory,
                                                    @Nonnull ScanProperties scanProperties) {
        return listDirectory(context, directory, null, scanProperties);
    }

    /**
     * Synchronously list the available paths from a directory.
     * @param context the context in which to perform database access
     * @param directory the path under which to list
     * @return a list of the paths that were found
     */
    @Nonnull
    public List<ResolvedKeySpacePath> listDirectory(@Nonnull FDBRecordContext context, @Nonnull String directory) {
        return listDirectory(context, directory, null, ScanProperties.FORWARD_SCAN);
    }


    /**
     * Get the root of this key space.
     * @return the root key space
     */
    public KeySpaceDirectory getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return root.toString();
    }

    /**
     * Creates a visually pleasing ASCII-art representation of the directory tree.
     * @param out the print destination
     * @throws IOException if there is a problem writing to the destination
     */
    public void toTree(Writer out) throws IOException {
        root.toTree(out);
    }
}
