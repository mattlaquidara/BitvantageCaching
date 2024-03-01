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
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;

@RequiredArgsConstructor
public class VersionedDynamoStoreSerializer<P extends PartitionKey, V> {

  private final DynamoStoreSerializer<P, V> serializer;

  public String getVersionKey() {
    return "version";
  }

  public Map<String, AttributeValue> serialize(final P partition, final V value)
      throws BitvantageStoreException {
    final Map<String, AttributeValue> item = serializer.serialize(partition, value);
    final byte[] uuidBytes = getUuidBytes(UUID.randomUUID());
    return ImmutableMap.<String, AttributeValue>builderWithExpectedSize(item.size() + 1)
        .putAll(item)
        .put(getVersionKey(), AttributeValue.fromB(SdkBytes.fromByteArray(uuidBytes)))
        .build();
  }

  public VersionedWrapper<V> deserializeValue(final Map<String, AttributeValue> item)
      throws BitvantageStoreException {
    final V value = serializer.deserializeValue(item);
    final UUID uuid = deserializeUuid(item);

    return new VersionedWrapper(uuid, value);
  }

  public UUID deserializeUuid(final Map<String, AttributeValue> item) {
    final AttributeValue versionAttribute = item.get(getVersionKey());
    final byte[] uuidBytes = versionAttribute.b().asByteArray();
    final ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
    final long high = buffer.getLong();
    final long low = buffer.getLong();
    return new UUID(high, low);
  }

  public Map<String, ExpectedAttributeValue> getExpectation(final UUID match) {
    return Collections.singletonMap(
        getVersionKey(),
        ExpectedAttributeValue.builder()
            .comparisonOperator(ComparisonOperator.EQ)
            .attributeValueList(AttributeValue.fromB(SdkBytes.fromByteArray(getUuidBytes(match))))
            .build());
  }

  public Map<String, ExpectedAttributeValue> getNonexistenceExpectation() {
    return Collections.singletonMap(
        getVersionKey(), ExpectedAttributeValue.builder().exists(false).build());
  }

  public byte[] getPartitionKey(final P key) throws BitvantageStoreException {
    return serializer.getPartitionKey(key);
  }

  public String getPartitionKeyName() throws BitvantageStoreException {
    return serializer.getPartitionKeyName();
  }

  private static byte[] getUuidBytes(final UUID uuid) {
    final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }
}
