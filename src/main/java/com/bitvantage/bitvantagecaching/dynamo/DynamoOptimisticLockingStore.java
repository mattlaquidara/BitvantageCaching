/*
 * Copyright 2020 Matt Laquidara.
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.OptimisticLockingStore;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoOptimisticLockingStore<K extends PartitionKey, V>
        implements OptimisticLockingStore<K, V> {

    private final String keyName;
    private final VersionedDynamoStoreSerializer<K, V> serializer;

    private final Table table;
    private final DynamoDB dynamo;

    public DynamoOptimisticLockingStore(
            final AmazonDynamoDB client, final String table,
            final VersionedDynamoStoreSerializer<K, V> serializer)
            throws BitvantageStoreException {
        this.dynamo = new DynamoDB(client);
        this.table = dynamo.getTable(table);
        this.keyName = serializer.getPartitionKeyName();
        this.serializer = serializer;
    }

    @Override
    public VersionedWrapper<V> get(final K key)
            throws BitvantageStoreException, InterruptedException {
        final Item result = retrieveItem(key);
        return result == null ? null : serializer.deserializeValue(result);
    }

    @Override
    public Optional<UUID> putOnMatch(final K key, final V value,
                                     final UUID match)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(key, value);

        final Expected expected = serializer.getExpectation(match);
        final PutItemSpec request = new PutItemSpec()
                .withExpected(expected)
                .withItem(item);

        try {
            table.putItem(request);
            final UUID uuid = serializer.deserializeUuid(item);

            return Optional.of(uuid);
        } catch (final ConditionalCheckFailedException e) {
            log.info("Condition checked failed: " +
                     "key={} value={} match={}.", key, value, match, e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<UUID> putIfAbsent(final K key, final V value)
            throws BitvantageStoreException, InterruptedException  {
        final Item item = serializer.serialize(key, value);

        final Expected expected = serializer.getNonexistenceExpectation();
        final PutItemSpec request = new PutItemSpec()
                .withExpected(expected)
                .withItem(item);

        try {
            table.putItem(request);
            final UUID uuid = serializer.deserializeUuid(item);

            return Optional.of(uuid);
        } catch (final ConditionalCheckFailedException e) {
            log.info("Nonexistince checked failed: " +
                     "key={} value={} match={}.", key, value, e);
            return Optional.empty();
        }
    }

    @Override
    public void put(final K key, final V value)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(key, value);
        table.putItem(item);
    }

    private Item retrieveItem(final K key) throws BitvantageStoreException {
        final byte[] keyBytes = serializer.getPartitionKey(key);

        final KeyAttribute hashKey = new KeyAttribute(keyName, keyBytes);
        final GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey(hashKey)
                .withConsistentRead(true);
        return table.getItem(spec);
    }

}
