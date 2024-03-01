/*
 * Copyright 2019 Matt Laquidara.
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
package com.bitvantage.bitvantagecaching.s3;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

@RequiredArgsConstructor
public class S3Store<P extends PartitionKey, V> implements Store<P, V> {

  private final S3Client s3;
  private final String bucket;
  private final S3Serializer<P, V> serializer;

  @Override
  public boolean containsKey(final P key) throws BitvantageStoreException, InterruptedException {
    final String keyString = serializer.getKey(key);

    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(keyString).build());
    } catch (final NoSuchKeyException e) {
      return false;
    }
    return true;
  }

  @Override
  public Optional<V> get(final P key) throws BitvantageStoreException, InterruptedException {
    try {
      final String keyString = serializer.getKey(key);

      final ResponseBytes<GetObjectResponse> object =
          s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(keyString).build());
      return Optional.of(serializer.deserializeValue(object.asByteArray()));
    } catch (final NoSuchKeyException e) {
      return Optional.empty();
    }
  }

  @Override
  public void put(final P key, final V value)
      throws BitvantageStoreException, InterruptedException {
    final String keyString = serializer.getKey(key);
    final byte[] bytes = serializer.serializeValue(value);

    s3.putObject(
        PutObjectRequest.builder()
            .bucket(bucket)
            .key(keyString)
            .contentLength((long) bytes.length)
            .build(),
        RequestBody.fromBytes(bytes));
  }

  @Override
  public void putAll(Map<P, V> entries) throws BitvantageStoreException, InterruptedException {
    for (final Map.Entry<P, V> entry : entries.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<P, V> getAll() throws BitvantageStoreException, InterruptedException {
    final ListObjectsV2Response listing =
        s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build());
    final List<S3Object> objectList = listing.contents();
    final ImmutableMap.Builder<P, V> builder = ImmutableMap.builder();
    for (final S3Object objectRecord : objectList) {
      final String keyString = objectRecord.key();
      final P key = serializer.deserializeKey(keyString);
      final ResponseBytes<GetObjectResponse> object =
          s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(keyString).build());
      final V value = serializer.deserializeValue(object.asByteArray());
      builder.put(key, value);
    }
    return builder.build();
  }

  @Override
  public boolean isEmpty() throws BitvantageStoreException, InterruptedException {
    final ListObjectsV2Response listing =
        s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).build());

    return !listing.hasContents();
  }

  @Override
  public void delete(final P key) throws BitvantageStoreException, InterruptedException {
    final String keyString = serializer.getKey(key);
    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(keyString).build());
  }
}
