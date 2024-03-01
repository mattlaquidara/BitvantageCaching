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
package com.bitvantage.bitvantagecaching.disk;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import com.bitvantage.bitvantagecaching.ValueSerializer;
import java.util.Optional;

@RequiredArgsConstructor
public class FlatFileStore<K extends PartitionKey, V> implements Store<K, V> {

  private final Path directory;
  private final FileManager<K> fileManager;
  private final ValueSerializer<V> serializer;

  @Override
  public boolean containsKey(final K key) throws BitvantageStoreException, InterruptedException {
    return Files.exists(directory.resolve(fileManager.getName(key)));
  }

  @Override
  public Optional<V> get(final K key) throws BitvantageStoreException, InterruptedException {
    try {
      return Optional.of(
          serializer.getValue(Files.readAllBytes(directory.resolve(fileManager.getName(key)))));
    } catch (final IOException e) {
      throw new BitvantageStoreException(e);
    }
  }

  @Override
  public void put(final K key, final V value)
      throws BitvantageStoreException, InterruptedException {
    try {
      Files.write(directory.resolve(fileManager.getName(key)), serializer.getBytes(value));
    } catch (final IOException e) {
      throw new BitvantageStoreException(e);
    }
  }

  @Override
  public void putAll(final Map<K, V> entries)
      throws BitvantageStoreException, InterruptedException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<K, V> getAll() throws BitvantageStoreException, InterruptedException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isEmpty() throws BitvantageStoreException, InterruptedException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void delete(K key) throws BitvantageStoreException, InterruptedException {
    try {
      Files.deleteIfExists(directory.resolve(fileManager.getName(key)));
    } catch (final IOException e) {
      throw new BitvantageStoreException(e);
    }
  }
}
