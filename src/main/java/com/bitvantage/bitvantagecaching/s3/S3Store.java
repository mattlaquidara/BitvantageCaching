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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Public Transit Analytics
 */
@RequiredArgsConstructor
public class S3Store<P extends PartitionKey, V> implements Store<P, V> {

    private final AmazonS3 s3;
    private final String bucket;
    private final S3Serializer<P, V> serializer;

    @Override
    public boolean containsKey(final P key) throws BitvantageStoreException,
            InterruptedException {
        final String keyString = serializer.getKey(key);
        return s3.doesObjectExist(bucket, keyString);
    }

    @Override
    public V get(final P key) throws BitvantageStoreException,
            InterruptedException {
        try {
            final String keyString = serializer.getKey(key);

            final S3Object object = s3.getObject(bucket, keyString);
            return serializer.deserializeValue(object);
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return null;
            }
            throw e;
        }

    }

    @Override
    public void put(final P key, final V value) throws BitvantageStoreException,
            InterruptedException {
        final String keyString = serializer.getKey(key);
        final byte[] bytes = serializer.serializeValue(value);
        final InputStream stream = new ByteArrayInputStream(bytes);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        s3.putObject(bucket, keyString, stream, metadata);
    }

    @Override
    public void putAll(Map<P, V> entries) throws BitvantageStoreException,
            InterruptedException {
        for (final Map.Entry<P, V> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Map<P, V> getAll() throws BitvantageStoreException,
            InterruptedException {
        final ObjectListing listing = s3.listObjects(bucket);
        final List<S3ObjectSummary> objectList = listing.getObjectSummaries();
        final ImmutableMap.Builder<P, V> builder = ImmutableMap.builder();
        for (final S3ObjectSummary summary : objectList) {
            final String keyString = summary.getKey();
            final S3Object object = s3.getObject(bucket, keyString);
            final P key = serializer.deserializeKey(keyString);
            final V value = serializer.deserializeValue(object);
            builder.put(key, value);
        }
        return builder.build();
    }

    @Override
    public boolean isEmpty() throws BitvantageStoreException,
            InterruptedException {
        final ObjectListing listing = s3.listObjects(bucket);
        final List<S3ObjectSummary> objectList = listing.getObjectSummaries();
        return objectList.isEmpty();
    }

    public void delete(final P key) throws BitvantageStoreException,
            InterruptedException {
        final String keyString = serializer.getKey(key);
        s3.deleteObject(bucket, keyString);
    }

}
