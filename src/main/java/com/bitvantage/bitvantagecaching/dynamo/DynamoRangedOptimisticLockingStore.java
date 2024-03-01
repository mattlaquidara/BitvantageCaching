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
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Slf4j
public class DynamoRangedOptimisticLockingStore<P extends PartitionKey, R extends RangeKey<R>, V>
    implements RangedOptimisticLockingStore<P, R, V> {

  private final String partitionKeyName;
  private final String rangeKeyName;
  private final VersionedDynamoRangedStoreSerializer<P, R, V> serializer;

  private final String tableName;
  private final DynamoDbClient dynamo;

  public DynamoRangedOptimisticLockingStore(
      final DynamoDbClient dynamo,
      final String tableName,
      final VersionedDynamoRangedStoreSerializer<P, R, V> serializer)
      throws BitvantageStoreException {
    this.dynamo = dynamo;
    this.tableName = tableName;
    this.partitionKeyName = serializer.getPartitionKeyName();
    this.rangeKeyName = serializer.getRangeKeyName();
    this.serializer = serializer;
  }

  @Override
  public Optional<VersionedWrapper<V>> get(final P partition, final R range)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> result = retrieveItem(partition, range);
    return result.isEmpty() ? Optional.empty() : Optional.of(serializer.deserializeValue(result));
  }

  @Override
  public Optional<UUID> putOnMatch(
      final P partition, final R range, final V value, final UUID match)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, range, value);

    final Map<String, ExpectedAttributeValue> expected = serializer.getExpectation(match);

    final PutItemRequest request =
        PutItemRequest.builder().tableName(tableName).item(item).expected(expected).build();

    try {
      dynamo.putItem(request);
      final UUID uuid = serializer.deserializeUuid(item);

      return Optional.of(uuid);
    } catch (final ConditionalCheckFailedException e) {
      log.info(
          "Condition checked failed: " + "key={}:{} value={} match={}.",
          partition,
          range,
          value,
          match,
          e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<UUID> putIfAbsent(final P partition, final R range, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, range, value);

    final Map<String, ExpectedAttributeValue> expected = serializer.getNonexistenceExpectation();

    final PutItemRequest request =
        PutItemRequest.builder().tableName(tableName).item(item).expected(expected).build();

    try {
      dynamo.putItem(request);
      final UUID uuid = serializer.deserializeUuid(item);

      return Optional.of(uuid);
    } catch (final ConditionalCheckFailedException e) {
      log.info(
          "Nonexistince checked failed: " + "key={}:{} value={} match={}.",
          partition,
          range,
          value,
          e);
      return Optional.empty();
    }
  }

  @Override
  public void put(final P partition, final R range, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, range, value);
    dynamo.putItem(PutItemRequest.builder().item(item).tableName(tableName).build());
  }

  @Override
  public NavigableMap<R, VersionedWrapper<V>> getPartition(final P partition)
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

  private NavigableMap<R, VersionedWrapper<V>> executeQuery(final QueryRequest request)
      throws BitvantageStoreException {
    final QueryResponse result = dynamo.query(request);

    final ImmutableSortedMap.Builder<R, VersionedWrapper<V>> builder =
        ImmutableSortedMap.naturalOrder();
    for (final Map<String, AttributeValue> item : result.items()) {
      final R rangeKey = serializer.deserializeRangeKey(item);
      final VersionedWrapper<V> wrapper = serializer.deserializeValue(item);

      builder.put(rangeKey, wrapper);
    }
    return builder.build();
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
            .consistentRead(true)
            .tableName(tableName)
            .build();
    return dynamo.getItem(request).item();
  }

  @Override
  public void delete(final P partition) throws BitvantageStoreException, InterruptedException {
    final byte[] partitionBytes = serializer.getPartitionKey(partition);
    final NavigableMap<R, VersionedWrapper<V>> items = getPartition(partition);

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
}
