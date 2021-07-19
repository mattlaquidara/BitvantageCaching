/*
 * Copyright 2018 Matt Laquidara.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bitvantage.bitvantagecaching.lmdb;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.RangedConditionedStore;
import com.google.common.collect.ImmutableSortedMap;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.KeyRangeType;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;
import com.bitvantage.bitvantagecaching.ValueSerializer;

/**
 * Uses LMDB to associate ranged keys with values.
 *
 * @author Matt Laquidara
 */
public class RangedNativeLmdbStore<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedConditionedStore<P, R, V> {

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final RangedKeyManager<P, R> keyManager;
    private final ValueSerializer<V> serializer;

    public RangedNativeLmdbStore(final Path path,
                                 final RangedKeyManager<P, R> keyManager,
                                 final ValueSerializer<V> serializer,
                                 final int readers) {
        env = Env.create().setMaxDbs(1).setMapSize(107374182400L)
                .setMaxReaders(readers).open(path.toFile());
        db = env.openDbi("DB", DbiFlags.MDB_CREATE);
        this.keyManager = keyManager;
        this.serializer = serializer;
    }

    @Override
    public NavigableMap<R, V> getValuesInRange(
            final P partition, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {
        final Txn<ByteBuffer> tx = env.txnRead();
        final ByteBuffer maxKeyBytes = getKeyBytes(partition, max);
        final ByteBuffer minKeyBytes = getKeyBytes(partition, min);
        final KeyRange range = new KeyRange(KeyRangeType.FORWARD_CLOSED,
                                            minKeyBytes, maxKeyBytes);

        final ImmutableSortedMap.Builder<R, V> builder
                = ImmutableSortedMap.naturalOrder();

        final CursorIterator<ByteBuffer> iterator = db.iterate(tx, range);

        try {

            while (iterator.hasNext()) {
                final CursorIterator.KeyVal<ByteBuffer> keyValue
                        = iterator.next();
                builder.put(getRangeKey(keyValue.key()),
                            getValue(keyValue.val()));
            }
            return builder.build();
        } finally {
            iterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public NavigableMap<R, V> getValuesAbove(final P partition, final R bottom)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, bottom, bottom.getRangeMax());
    }

    @Override
    public NavigableMap<R, V> getValuesBelow(final P partition, final R top)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, top.getRangeMin(), top);
    }

    @Override
    public NavigableMap<R, V> getNextValues(
            final P partition, R min, final int count)
            throws InterruptedException, BitvantageStoreException {
        final Txn<ByteBuffer> tx = env.txnRead();
        final ByteBuffer minKeyBytes = getKeyBytes(partition, min);
        final KeyRange range = new KeyRange(KeyRangeType.FORWARD_GREATER_THAN,
                                            minKeyBytes, null);

        final ImmutableSortedMap.Builder<R, V> builder
                = ImmutableSortedMap.naturalOrder();

        final CursorIterator<ByteBuffer> iterator = db.iterate(tx, range);

        try {
            int i = 0;
            while (iterator.hasNext() && i < count) {
                final CursorIterator.KeyVal<ByteBuffer> keyValue
                        = iterator.next();
                builder.put(getRangeKey(keyValue.key()),
                            getValue(keyValue.val()));
                i++;
            }
            return builder.build();
        } finally {
            iterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public NavigableMap<R, V> getHeadValues(P partition, int count) throws
            InterruptedException, BitvantageStoreException {
        final Txn<ByteBuffer> tx = env.txnRead();
        final KeyRange range = new KeyRange(KeyRangeType.FORWARD_ALL,
                                            null, null);

        final ImmutableSortedMap.Builder<R, V> builder
                = ImmutableSortedMap.naturalOrder();

        final CursorIterator<ByteBuffer> iterator = db.iterate(tx, range);

        try {
            int i = 0;
            while (iterator.hasNext() && i < count) {
                final CursorIterator.KeyVal<ByteBuffer> keyValue
                        = iterator.next();
                builder.put(getRangeKey(keyValue.key()),
                            getValue(keyValue.val()));
                i++;
            }
            return builder.build();
        } finally {
            iterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public void put(final P partition, final R range, final V value)
            throws BitvantageStoreException, InterruptedException {
        final ByteBuffer keyBytes = getKeyBytes(partition, range);
        final ByteBuffer valueBytes = getValueBytes(value);
        try {
            db.put(keyBytes, valueBytes);
        } finally {
        }
    }

    @Override
    public void putAll(P partition, Map<R, V> entries)
            throws BitvantageStoreException {
        final Txn tx = env.txnWrite();
        try {
            for (Map.Entry<R, V> entry : entries.entrySet()) {
                db.put(tx, getKeyBytes(partition, entry.getKey()),
                       getValueBytes(entry.getValue()));
            }
        } finally {
            tx.commit();
            tx.close();
        }
    }

    @Override
    public boolean isEmpty() {
        final Txn tx = env.txnRead();
        final Cursor cursor = db.openCursor(tx);

        try {
            boolean empty = !cursor.first();
            return empty;
        } finally {
            cursor.close();
            tx.commit();
            tx.close();
        }
    }

    private R getRangeKey(final ByteBuffer bytes)
            throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        bytes.get(byteArray, 0, bytes.capacity());
        final String keyString = new String(byteArray, StandardCharsets.UTF_8);
        return keyManager.materialize(keyString).getRange();
    }

    private P getPartitionKey(final ByteBuffer bytes)
            throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        bytes.get(byteArray, 0, bytes.capacity());
        final String keyString = new String(byteArray, StandardCharsets.UTF_8);
        return keyManager.materialize(keyString).getPartition();
    }

    private ByteBuffer getKeyBytes(final P partition, final R range) {
        final byte[] byteArray = keyManager.createKeyString(partition, range)
                .getBytes(StandardCharsets.UTF_8);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray).flip();
        return buffer;
    }

    private ByteBuffer getKeyStub(final P partition) {
        final byte[] byteArray = keyManager.createKeyStub(partition)
                .getBytes(StandardCharsets.UTF_8);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray).flip();
        return buffer;
    }

    private ByteBuffer getValueBytes(final V value)
            throws BitvantageStoreException {
        final byte[] byteArray = serializer.getBytes(value);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray).flip();
        return buffer;
    }

    private V getValue(final ByteBuffer bytes) throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        bytes.get(byteArray, 0, bytes.capacity());
        return serializer.getValue(byteArray);
    }

    @Override
    public NavigableMap<R, V> getPartition(final P partition) throws
            InterruptedException, BitvantageStoreException {
        final Txn<ByteBuffer> tx = env.txnRead();
        final ByteBuffer minKeyBytes = getKeyStub(partition);
        final KeyRange range = new KeyRange(KeyRangeType.FORWARD_AT_LEAST,
                                            minKeyBytes, null);

        final ImmutableSortedMap.Builder<R, V> builder
                = ImmutableSortedMap.naturalOrder();

        final CursorIterator<ByteBuffer> iterator = db.iterate(tx, range);

        try {
            while (iterator.hasNext()) {
                final CursorIterator.KeyVal<ByteBuffer> keyValue
                        = iterator.next();
                final ByteBuffer key = keyValue.key();
                final P keyPartition = getPartitionKey(key);
                if (!keyPartition.equals(partition)) {
                    break;
                }
                builder.put(getRangeKey(key),
                            getValue(keyValue.val()));
            }
            return builder.build();
        } finally {
            iterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public boolean putIfAbsent(final P partition, final R range,
                               final V value)
            throws BitvantageStoreException, InterruptedException {
        final ByteBuffer keyBytes = getKeyBytes(partition, range);
        final ByteBuffer valueBytes = getValueBytes(value);
        final Txn<ByteBuffer> tx = env.txnRead();
        try {
            final boolean wasAbsent = db.put(tx, keyBytes, valueBytes,
                                             PutFlags.MDB_NOOVERWRITE);
            return wasAbsent;
        } finally {
            tx.commit();
            tx.close();
        }
    }

    @Override
    public V get(final P partition, final R range) 
            throws BitvantageStoreException, InterruptedException {
       
        final ByteBuffer keyBytes = getKeyBytes(partition, range);
        final Txn<ByteBuffer> tx = env.txnRead();
        try {
            return serializer.getValue(db.get(tx, keyBytes).array());
        } finally {
            tx.commit();
            tx.close();
        }
    }

    @Override
    public boolean contains(P partition, R rangeValue) throws
            BitvantageStoreException, InterruptedException {
        return get(partition, rangeValue) != null;
    }

}
