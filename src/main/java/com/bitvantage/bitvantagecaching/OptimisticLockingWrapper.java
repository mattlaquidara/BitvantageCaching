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
package com.bitvantage.bitvantagecaching;

import java.util.Optional;
import java.util.UUID;

public class OptimisticLockingWrapper<K extends PartitionKey, V>
    implements OptimisticLockingStore<K, V> {

  final Store<K, VersionedWrapper<V>> store;

  public OptimisticLockingWrapper(final Store<K, VersionedWrapper<V>> store) {
    this.store = store;
  }

  @Override
  public Optional<VersionedWrapper<V>> get(final K key)
      throws BitvantageStoreException, InterruptedException {
    return store.get(key);
  }

  @Override
  public synchronized Optional<UUID> putOnMatch(final K key, final V value, final UUID match)
      throws BitvantageStoreException, InterruptedException {
    final Optional<VersionedWrapper<V>> optinalOldVersion = store.get(key);

    final Optional<UUID> response;
    if (optinalOldVersion.isEmpty()) {
      response = Optional.empty();
    } else if (optinalOldVersion.get().getVersion().equals(match)) {
      final VersionedWrapper<V> newWrapper = putAndGetVersion(key, value);
      response = Optional.of(newWrapper.getVersion());
    } else {
      response = Optional.empty();
    }

    return response;
  }

  @Override
  public synchronized Optional<UUID> putIfAbsent(final K key, final V value)
      throws BitvantageStoreException, InterruptedException {
    if (!store.containsKey(key)) {
      final VersionedWrapper<V> newWrapper = putAndGetVersion(key, value);
      return Optional.of(newWrapper.getVersion());
    }
    return Optional.empty();
  }

  @Override
  public void put(K key, V value) throws BitvantageStoreException, InterruptedException {
    store.put(key, new VersionedWrapper(UUID.randomUUID(), value));
  }

  private VersionedWrapper<V> putAndGetVersion(K key, V value)
      throws BitvantageStoreException, InterruptedException {
    final VersionedWrapper<V> wrapper = new VersionedWrapper(UUID.randomUUID(), value);
    store.put(key, wrapper);
    return wrapper;
  }
}
