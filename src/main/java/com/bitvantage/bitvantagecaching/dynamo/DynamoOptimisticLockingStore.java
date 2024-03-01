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

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.OptimisticLockingStore;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@Slf4j
public class DynamoOptimisticLockingStore<K extends PartitionKey, V>
    implements OptimisticLockingStore<K, V> {

  private final String keyName;
  private final VersionedDynamoStoreSerializer<K, V> serializer;

  private final String tableName;
  private final DynamoDbClient dynamo;

  public DynamoOptimisticLockingStore(
      final DynamoDbClient client,
      final String tableName,
      final VersionedDynamoStoreSerializer<K, V> serializer)
      throws BitvantageStoreException {
    this.dynamo = client;
    this.tableName = tableName;
    this.keyName = serializer.getPartitionKeyName();
    this.serializer = serializer;
  }

  @Override
  public Optional<VersionedWrapper<V>> get(final K key)
      throws BitvantageStoreException, InterruptedException {

    final Map<String, AttributeValue> result = retrieveItem(key);
    return result.isEmpty() ? Optional.empty() : Optional.ofNullable(serializer.deserializeValue(result));
  }

  @Override
  public Optional<UUID> putOnMatch(final K key, final V value, final UUID match)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(key, value);

    final Map<String, ExpectedAttributeValue> expected = serializer.getExpectation(match);

    final PutItemRequest request =
        PutItemRequest.builder().tableName(tableName).item(item).expected(expected).build();

    try {
      dynamo.putItem(request);
      final UUID uuid = serializer.deserializeUuid(item);

      return Optional.of(uuid);
    } catch (final ConditionalCheckFailedException e) {
      log.info("Condition checked failed: " + "key={} value={} match={}.", key, value, match, e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<UUID> putIfAbsent(final K key, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(key, value);

    final Map<String, ExpectedAttributeValue> expected = serializer.getNonexistenceExpectation();

    final PutItemRequest request =
        PutItemRequest.builder().tableName(tableName).item(item).expected(expected).build();

    try {
      dynamo.putItem(request);
      final UUID uuid = serializer.deserializeUuid(item);

      return Optional.of(uuid);
    } catch (final ConditionalCheckFailedException e) {
      log.info("Nonexistince checked failed: " + "key={} value={} match={}.", key, value, e);
      return Optional.empty();
    }
  }

  @Override
  public void put(final K key, final V value)
      throws BitvantageStoreException, InterruptedException {
    final Map<String, AttributeValue> item = serializer.serialize(key, value);
    dynamo.putItem(PutItemRequest.builder().tableName(tableName).item(item).build());
  }

  private Map<String, AttributeValue> retrieveItem(final K key) throws BitvantageStoreException {
    final byte[] keyBytes = serializer.getPartitionKey(key);

    final Map<String, AttributeValue> dynamoKey =
        Collections.singletonMap(keyName, AttributeValue.fromB(SdkBytes.fromByteArray(keyBytes)));

    final GetItemRequest request =
        GetItemRequest.builder().key(dynamoKey).tableName(tableName).build();

    final GetItemResponse response = dynamo.getItem(request);

    return response.item();
  }
}
