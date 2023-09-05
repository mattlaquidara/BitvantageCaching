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
package com.bitvantage.bitvantagecaching.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;

/**
 * @author Matt Laquidara
 */
public class DynamoStore<P extends PartitionKey, V> implements Store<P, V> {

  private static final int BATCH_SIZE = 25;

  private final String keyName;
  private final DynamoStoreSerializer<P, V> serializer;

  private final Table table;
  private final DynamoDB dynamo;

  public DynamoStore(
      final AmazonDynamoDB client, final String table, final DynamoStoreSerializer<P, V> serializer)
      throws BitvantageStoreException {
    this.dynamo = new DynamoDB(client);
    this.table = dynamo.getTable(table);
    this.keyName = serializer.getPartitionKeyName();
    this.serializer = serializer;
  }

  @Override
  public boolean containsKey(final P key) throws BitvantageStoreException, InterruptedException {
    return retrieveItem(key) != null;
  }

  @Override
  public V get(final P key) throws BitvantageStoreException, InterruptedException {

    final Item result = retrieveItem(key);
    return result == null ? null : serializer.deserializeValue(result);
  }

  @Override
  public void put(final P key, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Item item = serializer.serialize(key, value);
    table.putItem(item);
  }

  @Override
  public void putAll(final Map<P, V> entries)
      throws BitvantageStoreException, InterruptedException {
    final ImmutableList.Builder<Item> builder = ImmutableList.builder();
    for (final Map.Entry<P, V> entry : entries.entrySet()) {
      builder.add(serializer.serialize(entry.getKey(), entry.getValue()));
    }
    final List<Item> items = builder.build();
    final int total = items.size();

    int start = 0;

    while (start < total) {
      final int end = Math.min(total, start + BATCH_SIZE);
      final List<Item> subItems = items.subList(start, end);

      final TableWriteItems writeRequest =
          new TableWriteItems(table.getTableName()).withItemsToPut(subItems);
      final BatchWriteItemOutcome outcome = dynamo.batchWriteItem(writeRequest);
      Map<String, List<WriteRequest>> unprocessed = outcome.getUnprocessedItems();
      while (unprocessed.size() > 0) {
        Thread.sleep(1000);
        final BatchWriteItemOutcome partialOutcome = dynamo.batchWriteItemUnprocessed(unprocessed);
        unprocessed = partialOutcome.getUnprocessedItems();
      }
      start = end;
    }
  }

  @Override
  public Map<P, V> getAll() throws BitvantageStoreException, InterruptedException {
    final ItemCollection<ScanOutcome> result = table.scan();
    final ImmutableMap.Builder<P, V> builder = ImmutableMap.builder();
    for (final Item item : result) {
      final P key = serializer.deserializeKey(item);
      final V value = serializer.deserializeValue(item);
      builder.put(key, value);
    }
    return builder.build();
  }

  @Override
  public boolean isEmpty() throws BitvantageStoreException, InterruptedException {
    return false;
  }

  private Item retrieveItem(final P key) throws BitvantageStoreException {
    final byte[] keyBytes = serializer.getPartitionKey(key);
    final KeyAttribute hashKey = new KeyAttribute(keyName, keyBytes);
    final GetItemSpec spec = new GetItemSpec().withPrimaryKey(hashKey).withConsistentRead(true);

    return table.getItem(spec);
  }

  @Override
  public void delete(final P key) throws BitvantageStoreException, InterruptedException {
    final byte[] keyBytes = serializer.getPartitionKey(key);
    final KeyAttribute hashKey = new KeyAttribute(keyName, keyBytes);
    final DeleteItemSpec spec = new DeleteItemSpec().withPrimaryKey(hashKey);
    table.deleteItem(spec);
  }
}
