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
package com.bitvantage.bitvantagecaching;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RangedOptimisticLockingWrapper<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedOptimisticLockingStore<P, R, V> {

    private final RangedStore<P, R, VersionedWrapper<V>> store;

    @Override
    public Optional<VersionedWrapper<V>> get(final P partition, final R range)
            throws BitvantageStoreException, InterruptedException {
        return store.get(partition, range);
    }

    @Override
    public NavigableMap<R, VersionedWrapper<V>> getPartition(final P partition)
            throws
            InterruptedException, BitvantageStoreException {
        return store.getPartition(partition);
    }

    @Override
    public void put(P partition, R range, V value) throws
            BitvantageStoreException, InterruptedException {
        store.put(partition, range,
                  new VersionedWrapper(UUID.randomUUID(), value));
    }

    @Override
    public Optional<UUID> putIfAbsent(P partition, R range, V value) throws
            BitvantageStoreException, InterruptedException {
        if (!store.contains(partition, range)) {
            final VersionedWrapper<V> newWrapper
                    = putAndGetVersion(partition, range, value);
            return Optional.of(newWrapper.getVersion());
        }
        return Optional.empty();
    }

    @Override
    public Optional<UUID> putOnMatch(P partition, R range, V value, UUID match)
            throws BitvantageStoreException, InterruptedException {
        final Optional<VersionedWrapper<V>> oldVersion = store.get(partition, range);
        
        final Optional<UUID> newVersion;
        if (oldVersion.isEmpty()) {
          newVersion = Optional.empty();
        } else if (oldVersion.get().getVersion().equals(match)) {
            final VersionedWrapper<V> newWrapper = putAndGetVersion(
                    partition, range, value);
            newVersion = Optional.of(newWrapper.getVersion());
        } else {
          newVersion = Optional.empty();
        }
        return newVersion;
    }
    
    private VersionedWrapper<V> putAndGetVersion(P partition, R range, V value)
            throws BitvantageStoreException,
            InterruptedException {
        final VersionedWrapper<V> wrapper
                = new VersionedWrapper(UUID.randomUUID(), value);
        store.put(partition, range, wrapper);
        return wrapper;
    }

  @Override
  public void delete(final P partition) throws BitvantageStoreException, InterruptedException {
    store.delete(partition);
  }

}
