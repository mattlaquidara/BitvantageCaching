/*
 * Copyright 2021 Matt Laquidara.
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
package com.bitvantage.bitvantagecaching.dynamo;

import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import java.nio.ByteBuffer;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VersionedDynamoRangedStoreSerializer<P extends PartitionKey, R extends RangeKey<R>, V> {

    private final DynamoRangedStoreSerializer<P, R, V> serializer;

    public String getVersionKey() {
        return "version";
    }

    public Item serialize(final P partition, final R range, final V value)
            throws BitvantageStoreException {
        final Item item = serializer.serialize(partition, range, value);
        final byte[] uuidBytes = getUuidBytes(UUID.randomUUID());
        return item.withBinary(getVersionKey(), uuidBytes);
    }
    
    public R deserializeRangeKey(final Item item) 
            throws BitvantageStoreException {
        return serializer.deserializeRangeKey(item);
    }

    public VersionedWrapper<V> deserializeValue(final Item item)
            throws BitvantageStoreException {
        final V value = serializer.deserializeValue(item);
        final UUID uuid = deserializeUuid(item);

        return new VersionedWrapper(uuid, value);
    }

    public UUID deserializeUuid(final Item item) {
        final byte[] uuidBytes = item.getBinary(getVersionKey());
        final ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        final long high = buffer.getLong();
        final long low = buffer.getLong();
        return new UUID(high, low);
    }

    public Expected getExpectation(final UUID match) {
        return new Expected(getVersionKey()).eq(getUuidBytes(match));
    }

    public Expected getNonexistenceExpectation() {
        return new Expected(getVersionKey()).notExist();
    }

    public byte[] getPartitionKey(final P key) throws BitvantageStoreException {
        return serializer.getPartitionKey(key);
    }

    public String getPartitionKeyName() throws BitvantageStoreException {
        return serializer.getPartitionKeyName();
    }

    public byte[] getRangeKey(final R key) throws BitvantageStoreException {
        return serializer.getRangeKey(key);
    }

    public String getRangeKeyName() throws BitvantageStoreException {
        return serializer.getRangeKeyName();
    }

    private static byte[] getUuidBytes(final UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

}
