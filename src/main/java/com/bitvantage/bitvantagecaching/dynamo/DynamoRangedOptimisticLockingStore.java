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

import com.bitvantage.bitvantagecaching.RangedOptimisticLockingStore;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import com.google.common.collect.ImmutableSortedMap;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Public Transit Analytics
 */
@Slf4j
public class DynamoRangedOptimisticLockingStore<P extends PartitionKey, R extends RangeKey<R>, V> implements RangedOptimisticLockingStore<P, R, V> {

    private final String partitionKeyName;
    private final String rangeKeyName;
    private final VersionedDynamoRangedStoreSerializer<P, R, V> serializer;

    private final Table table;
    private final DynamoDB dynamo;

    public DynamoRangedOptimisticLockingStore(
            final AmazonDynamoDB client, final String table,
            final VersionedDynamoRangedStoreSerializer<P, R, V> serializer)
            throws BitvantageStoreException {
        this.dynamo = new DynamoDB(client);
        this.table = dynamo.getTable(table);
        this.partitionKeyName = serializer.getPartitionKeyName();
        this.rangeKeyName = serializer.getRangeKeyName();
        this.serializer = serializer;
    }

    @Override
    public VersionedWrapper<V> get(final P partition, final R range)
            throws BitvantageStoreException, InterruptedException {
        final Item result = retrieveItem(partition, range);
        return result == null ? null : serializer.deserializeValue(result);
    }

    @Override
    public Optional<UUID> putOnMatch(
            final P partition, final R range, final V value, final UUID match)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(partition, range, value);

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
                     "key={}:{} value={} match={}.", partition, range, value,
                     match, e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<UUID> putIfAbsent(final P partition, final R range,
                                      final V value)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(partition, range, value);

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
                     "key={}:{} value={} match={}.", partition, range, value,
                     e);
            return Optional.empty();
        }
    }

    @Override
    public void put(final P partition, final R range, final V value)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(partition, range, value);
        table.putItem(item);
    }

    @Override
    public NavigableMap<R, VersionedWrapper<V>> getPartition(final P partition) throws
            InterruptedException, BitvantageStoreException {
        final byte[] hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(partitionKeyName,
                                                      hashValue);
        final QuerySpec querySpec = new QuerySpec()
                .withHashKey(hashKey)
                .withConsistentRead(true);

        return executeQuery(querySpec);
    }

    private NavigableMap<R, VersionedWrapper<V>> executeQuery(final QuerySpec querySpec)
            throws BitvantageStoreException {
        final ItemCollection<QueryOutcome> result = table.query(querySpec);

        final ImmutableSortedMap.Builder<R, VersionedWrapper<V>> builder
                = ImmutableSortedMap.naturalOrder();
        for (final Item item : result) {
            final R rangeKey = serializer.deserializeRangeKey(item);
            final VersionedWrapper<V> wrapper = serializer.deserializeValue(item);

            builder.put(rangeKey, wrapper);
        }
        return builder.build();
    }

    private Item retrieveItem(final P partition, final R range)
            throws BitvantageStoreException {
        final byte[] partitionBytes = serializer.getPartitionKey(partition);
        final byte[] rangeBytes = serializer.getRangeKey(range);

        final GetItemSpec spec = new GetItemSpec()
                .withPrimaryKey(partitionKeyName, partitionBytes, rangeKeyName, 
                        rangeBytes)
                .withConsistentRead(true);
        return table.getItem(spec);
    }

}
