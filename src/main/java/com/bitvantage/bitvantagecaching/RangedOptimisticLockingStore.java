/*
 * Copyright 2021 Public Transit Analytics.
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

/**
 *
 * @author Public Transit Analytics
 */
public interface RangedOptimisticLockingStore<P extends PartitionKey, R extends RangeKey<R>, V> {

    VersionedWrapper<V> get(final P partition, final R range) throws
            BitvantageStoreException, InterruptedException;

    NavigableMap<R, VersionedWrapper<V>> getPartition(final P partition) throws
            InterruptedException, BitvantageStoreException;

    void put(final P partition, final R range, final V value) throws
            BitvantageStoreException, InterruptedException;

    Optional<UUID> putIfAbsent(final P partition, final R range, final V value)
            throws BitvantageStoreException, InterruptedException;

    Optional<UUID> putOnMatch(final P partition, final R range, final V value,
                              final UUID match) throws BitvantageStoreException,
            InterruptedException;
    
}