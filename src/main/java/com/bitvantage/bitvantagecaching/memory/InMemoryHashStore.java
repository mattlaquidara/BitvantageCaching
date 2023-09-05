/*
 * Copyright 2017 Matt Laquidara.
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
package com.bitvantage.bitvantagecaching.memory;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryHashStore<K extends PartitionKey, V> implements Store<K, V> {

  private final Map<K, V> map;

  public InMemoryHashStore() {
    map = new ConcurrentHashMap<>();
  }

  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Map<K, V> getAll() {
    return ImmutableMap.copyOf(map);
  }

  @Override
  public void putAll(final Map<K, V> entries) {
    map.putAll(entries);
  }

  @Override
  public void delete(final K key) throws BitvantageStoreException, InterruptedException {
    map.remove(key);
  }
}
