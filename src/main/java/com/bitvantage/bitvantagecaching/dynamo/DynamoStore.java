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

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoStore<P extends PartitionKey, V> implements Store<P, V> {

  private static final int BATCH_SIZE = 25;

  private final String keyName;
  private final DynamoStoreSerializer<P, V> serializer;

  private final String tableName;
  private final DynamoDbClient dynamo;

  public DynamoStore(
      final DynamoDbClient dynamo,
      final String tableName,
      final DynamoStoreSerializer<P, V> serializer)
      throws BitvantageStoreException {
    this.dynamo = dynamo;
    this.tableName = tableName;
    this.keyName = serializer.getPartitionKeyName();
    this.serializer = serializer;
  }

  @Override
  public boolean containsKey(final P key) throws BitvantageStoreException, InterruptedException {
    return retrieveItem(key) != null;
  }

  @Override
  public Optional<V> get(final P key) throws BitvantageStoreException, InterruptedException {

    final Map<String, AttributeValue> result = retrieveItem(key);
    return result.isEmpty() ? Optional.empty() : Optional.of(serializer.deserializeValue(result));
  }

  @Override
  public void put(final P key, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(key, value);
    dynamo.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
  }

  @Override
  public void putAll(final Map<P, V> entries)
      throws BitvantageStoreException, InterruptedException {
    final ImmutableList.Builder<Map<String, AttributeValue>> builder = ImmutableList.builder();
    for (final Map.Entry<P, V> entry : entries.entrySet()) {
      builder.add(serializer.serialize(entry.getKey(), entry.getValue()));
    }
    final List<Map<String, AttributeValue>> items = builder.build();
    final int total = items.size();

    int start = 0;

    while (start < total) {
      final int end = Math.min(total, start + BATCH_SIZE);
      final List<Map<String, AttributeValue>> subItems = items.subList(start, end);
      final List<WriteRequest> writes =
          subItems.stream()
              .map(
                  item ->
                      WriteRequest.builder()
                          .putRequest(PutRequest.builder().item(item).build())
                          .build())
              .collect(ImmutableList.toImmutableList());

      final BatchWriteItemResponse outcome =
          dynamo.batchWriteItem(
              BatchWriteItemRequest.builder()
                  .requestItems(Collections.singletonMap(tableName, writes))
                  .build());

      Map<String, List<WriteRequest>> unprocessed = outcome.unprocessedItems();
      while (!unprocessed.isEmpty()) {
        Thread.sleep(1000);
        final BatchWriteItemResponse partialOutcome =
            dynamo.batchWriteItem(
                BatchWriteItemRequest.builder().requestItems(unprocessed).build());
        unprocessed = partialOutcome.unprocessedItems();
      }
      start = end;
    }
  }

  @Override
  public Map<P, V> getAll() throws BitvantageStoreException, InterruptedException {
    final ScanResponse result = dynamo.scan(ScanRequest.builder().tableName(tableName).build());
    final ImmutableMap.Builder<P, V> builder = ImmutableMap.builder();
    for (final Map<String, AttributeValue> item : result.items()) {
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

  private Map<String, AttributeValue> retrieveItem(final P key) throws BitvantageStoreException {
    final byte[] keyBytes = serializer.getPartitionKey(key);
    final GetItemRequest request =
        GetItemRequest.builder()
            .key(
                Collections.singletonMap(
                    keyName, AttributeValue.fromB(SdkBytes.fromByteArray(keyBytes))))
            .tableName(tableName)
            .build();
    final GetItemResponse response = dynamo.getItem(request);
    return response.item();
  }

  @Override
  public void delete(final P key) throws BitvantageStoreException, InterruptedException {
    final byte[] keyBytes = serializer.getPartitionKey(key);
    final DeleteItemRequest request =
        DeleteItemRequest.builder()
            .tableName(tableName)
            .key(
                Collections.singletonMap(
                    keyName, AttributeValue.fromB(SdkBytes.fromByteArray(keyBytes))))
            .build();
    dynamo.deleteItem(request);
  }
}
