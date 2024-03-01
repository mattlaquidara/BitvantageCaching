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
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.RangedConditionedStore;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Slf4j
public class DynamoRangedStore<P extends PartitionKey, R extends RangeKey<R>, V>
    implements RangedConditionedStore<P, R, V> {

  private static final int BATCH_SIZE = 25;

  private final String partitionKeyName;
  private final String rangeKeyName;
  private final DynamoRangedStoreSerializer<P, R, V> serializer;

  private final String tableName;
  private final DynamoDbClient dynamo;

  public DynamoRangedStore(
      final DynamoDbClient dynamo,
      final String tableName,
      final DynamoRangedStoreSerializer<P, R, V> serializer) {
    this.dynamo = dynamo;
    this.tableName = tableName;
    this.partitionKeyName = serializer.getPartitionKeyName();
    this.rangeKeyName = serializer.getRangeKeyName();
    this.serializer = serializer;
  }

  @Override
  public NavigableMap<R, V> getValuesInRange(final P partition, final R min, final R max)
      throws InterruptedException, BitvantageStoreException {
    final byte[] hashValue = serializer.getPartitionKey(partition);

    final byte[] minValue = serializer.getRangeKey(min);
    final byte[] maxValue = serializer.getRangeKey(max);
    final QueryRequest request =
        QueryRequest.builder()
            .tableName(tableName)
            .keyConditions(
                ImmutableMap.of(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build(),
                    rangeKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.BETWEEN)
                        .attributeValueList(
                            AttributeValue.fromB(SdkBytes.fromByteArray(minValue)),
                            AttributeValue.fromB(SdkBytes.fromByteArray(maxValue)))
                        .build()))
            .build();
    return executeQuery(request);
  }

  @Override
  public NavigableMap<R, V> getValuesAbove(final P partition, final R min)
      throws InterruptedException, BitvantageStoreException {
    final byte[] hashValue = serializer.getPartitionKey(partition);

    final byte[] minValue = serializer.getRangeKey(min);

    final QueryRequest request =
        QueryRequest.builder()
            .tableName(tableName)
            .keyConditions(
                ImmutableMap.of(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build(),
                    rangeKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.GE)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(minValue)))
                        .build()))
            .build();
    return executeQuery(request);
  }

  @Override
  public NavigableMap<R, V> getValuesBelow(final P partition, final R max)
      throws InterruptedException, BitvantageStoreException {
    final byte[] hashValue = serializer.getPartitionKey(partition);

    final byte[] maxValue = serializer.getRangeKey(max);

    final QueryRequest request =
        QueryRequest.builder()
            .tableName(tableName)
            .keyConditions(
                ImmutableMap.of(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build(),
                    rangeKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.LE)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(maxValue)))
                        .build()))
            .build();
    return executeQuery(request);
  }

  @Override
  public NavigableMap<R, V> getNextValues(final P partition, final R min, final int count)
      throws InterruptedException, BitvantageStoreException {
    final byte[] hashValue = serializer.getPartitionKey(partition);

    final byte[] minValue = serializer.getRangeKey(min);

    final QueryRequest request =
        QueryRequest.builder()
            .limit(count)
            .tableName(tableName)
            .keyConditions(
                ImmutableMap.of(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build(),
                    rangeKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.GT)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(minValue)))
                        .build()))
            .build();
    return executeQuery(request);
  }

  @Override
  public NavigableMap<R, V> getHeadValues(final P partition, final int count)
      throws InterruptedException, BitvantageStoreException {

    final byte[] hashValue = serializer.getPartitionKey(partition);

    final QueryRequest request =
        QueryRequest.builder()
            .limit(count)
            .tableName(tableName)
            .keyConditions(
                ImmutableMap.of(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build()))
            .build();
    return executeQuery(request);
  }

  @Override
  public void put(final P partition, final R rangeValue, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, rangeValue, value);
    dynamo.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
  }

  @Override
  public void putAll(final P partition, final Map<R, V> entries) throws InterruptedException {
    final ImmutableList.Builder<Map<String, AttributeValue>> builder = ImmutableList.builder();
    for (final Map.Entry<R, V> entry : entries.entrySet()) {
      builder.add(serializer.serialize(partition, entry.getKey(), entry.getValue()));
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
  public boolean isEmpty() {
    return false;
  }

  @Override
  public NavigableMap<R, V> getPartition(final P partition)
      throws InterruptedException, BitvantageStoreException {
    final byte[] hashValue = serializer.getPartitionKey(partition);

    final QueryRequest request =
        QueryRequest.builder()
            .tableName(tableName)
            .keyConditions(
                Collections.singletonMap(
                    partitionKeyName,
                    Condition.builder()
                        .comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(hashValue)))
                        .build()))
            .build();

    return executeQuery(request);
  }

  @Override
  public boolean putIfAbsent(final P partition, final R range, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, range, value);
    try {

      dynamo.putItem(
          PutItemRequest.builder()
              .item(item)
              .expected(
                  ImmutableMap.of(
                      partitionKeyName,
                      ExpectedAttributeValue.builder().exists(false).build(),
                      rangeKeyName,
                      ExpectedAttributeValue.builder().exists(false).build()))
              .build());
    } catch (final ConditionalCheckFailedException e) {
      log.info("Could not put, item was present.", e);
      return false;
    }
    return true;
  }

  private NavigableMap<R, V> executeQuery(final QueryRequest request)
      throws BitvantageStoreException {
    final QueryResponse result = dynamo.query(request);

    final ImmutableSortedMap.Builder<R, V> builder = ImmutableSortedMap.naturalOrder();
    for (final Map<String, AttributeValue> item : result.items()) {
      final R rangeKey = serializer.deserializeRangeKey(item);
      final V value = serializer.deserializeValue(item);

      builder.put(rangeKey, value);
    }
    return builder.build();
  }

  @Override
  public Optional<V> get(final P partition, final R range)
      throws BitvantageStoreException, InterruptedException {

    final Map<String, AttributeValue> item = retrieveItem(partition, range);
    return item.isEmpty() ? Optional.empty() : Optional.of(serializer.deserializeValue(item));
  }

  @Override
  public boolean contains(final P partition, final R range)
      throws BitvantageStoreException, InterruptedException {

    return retrieveItem(partition, range) != null;
  }

  @Override
  public void delete(final P partition) throws BitvantageStoreException, InterruptedException {
    final byte[] partitionBytes = serializer.getPartitionKey(partition);
    final NavigableMap<R, V> items = getPartition(partition);

    final ImmutableList.Builder<WriteRequest> builder = ImmutableList.<WriteRequest>builder();
    for (final R r : items.keySet()) {
      builder.add(
          WriteRequest.builder()
              .deleteRequest(
                  DeleteRequest.builder()
                      .key(
                          ImmutableMap.of(
                              partitionKeyName,
                              AttributeValue.fromB(SdkBytes.fromByteArray(partitionBytes)),
                              rangeKeyName,
                              AttributeValue.fromB(
                                  SdkBytes.fromByteArray(serializer.getRangeKey(r)))))
                      .build())
              .build());
    }

    dynamo.batchWriteItem(
        BatchWriteItemRequest.builder()
            .requestItems(Collections.singletonMap(tableName, builder.build()))
            .build());
  }

  private Map<String, AttributeValue> retrieveItem(final P partition, final R range)
      throws BitvantageStoreException {
    final byte[] partitionBytes = serializer.getPartitionKey(partition);
    final byte[] rangeBytes = serializer.getRangeKey(range);

    final GetItemRequest request =
        GetItemRequest.builder()
            .key(
                ImmutableMap.of(
                    partitionKeyName,
                    AttributeValue.fromB(SdkBytes.fromByteArray(partitionBytes)),
                    rangeKeyName,
                    AttributeValue.fromB(SdkBytes.fromByteArray(rangeBytes))))
            .tableName(tableName)
            .build();
    return dynamo.getItem(request).item();
  }
}
