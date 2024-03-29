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

public interface OptimisticLockingStore<K extends PartitionKey, V> {

    public Optional<VersionedWrapper<V>> get(final K key) throws BitvantageStoreException,
            InterruptedException;

    Optional<UUID> putOnMatch(K key, V value, UUID match)
            throws BitvantageStoreException, InterruptedException;
    
    Optional<UUID> putIfAbsent(K key, V value)
            throws BitvantageStoreException, InterruptedException;

    void put(K key, V value)
            throws BitvantageStoreException, InterruptedException;

}
