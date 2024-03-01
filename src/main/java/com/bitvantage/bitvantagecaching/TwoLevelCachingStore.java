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
package com.bitvantage.bitvantagecaching;

import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TwoLevelCachingStore<K extends PartitionKey, V> implements Store<K, V> {

  private final Store<K, V> store;
  protected final TwoLevelCache<K, V> cache;

  @Override
  public boolean containsKey(K key) throws InterruptedException, BitvantageStoreException {
    if (cache.get(key) != null) {
      return true;
    }
    return store.containsKey(key);
  }

  @Override
  public Optional<V> get(K key) throws InterruptedException, BitvantageStoreException {
    final Optional<V> returnValue;
    
    final Optional<V> cacheValue = cache.get(key);
    if (cacheValue.isEmpty()) {
      final Optional<V> storeValue = store.get(key);
      if (storeValue.isPresent()) {
        cache.put(key, storeValue.get());
      }
      return storeValue;
    } else {
      returnValue = cacheValue;
    }
    
    return returnValue;
  }

  @Override
  public void put(K key, V value) throws InterruptedException, BitvantageStoreException {
    store.put(key, value);
  }

  @Override
  public boolean isEmpty() throws InterruptedException, BitvantageStoreException {
    return store.isEmpty();
  }

  @Override
  public Map<K, V> getAll() throws InterruptedException, BitvantageStoreException {
    return store.getAll();
  }

  @Override
  public void putAll(Map<K, V> entries) throws InterruptedException, BitvantageStoreException {
    store.putAll(entries);
  }

  @Override
  public void delete(final K key) throws BitvantageStoreException, InterruptedException {
    cache.invalidate(key);
    store.delete(key);
  }
}
