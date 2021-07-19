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
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.RangedConditionedStore;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 *
 * @author Matt Laquidara
 */
public class InMemoryPartitionedStore<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedConditionedStore<P, R, V> {

    final Map<P, NavigableMap<R, V>> partitionedMap;

    public InMemoryPartitionedStore() {
        partitionedMap = new HashMap<>();
    }

    @Override
    public synchronized NavigableMap<R, V> getValuesInRange(
            final P partition, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {
        final NavigableMap<R, V> map = partitionedMap.getOrDefault(
                partition, Collections.emptyNavigableMap());
        return ImmutableSortedMap.copyOf(map.subMap(min, true, max, true));
    }

    @Override
    public NavigableMap<R, V> getValuesAbove(final P partition, final R min)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, min, min.getRangeMax());
    }

    @Override
    public NavigableMap<R, V> getValuesBelow(final P partition, final R max)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, max.getRangeMin(), max);
    }

    @Override
    public synchronized void put(final P partition, final R key, final V value)
            throws BitvantageStoreException, InterruptedException {
        final NavigableMap<R, V> map = partitionedMap.getOrDefault(
                partition, new TreeMap<>());
        map.put(key, value);
        partitionedMap.put(partition, map);
    }

    @Override
    public synchronized boolean isEmpty() {
        return partitionedMap.isEmpty();
    }

    @Override
    public synchronized void putAll(final P partition, final Map<R, V> values) {
        final NavigableMap<R, V> map = partitionedMap.getOrDefault(
                partition, new TreeMap<>());
        map.putAll(values);
        partitionedMap.put(partition, map);
    }

    @Override
    public synchronized NavigableMap<R, V> getNextValues(
            final P partition, final R min, final int count)
            throws InterruptedException, BitvantageStoreException {
        final NavigableMap<R, V> map = partitionedMap.getOrDefault(
                partition, new TreeMap<>());
        final NavigableMap<R, V> subMap
                = map.subMap(min, false, map.lastKey(), true);

        final ImmutableSortedMap.Builder builder
                = ImmutableSortedMap.naturalOrder();
        subMap.entrySet().stream().limit(count).forEach(builder::put);
        return builder.build();
    }

    @Override
    public synchronized NavigableMap<R, V> getHeadValues(
            final P partition, final int count)
            throws InterruptedException, BitvantageStoreException {
        final NavigableMap<R, V> map = partitionedMap.getOrDefault(
                partition, new TreeMap<>());

        final ImmutableSortedMap.Builder builder
                = ImmutableSortedMap.naturalOrder();
        map.entrySet().stream().limit(count).forEach(builder::put);
        return builder.build();

    }

    @Override
    public synchronized NavigableMap<R, V> getPartition(final P partition)
            throws InterruptedException, BitvantageStoreException {
        return partitionedMap.getOrDefault(partition, new TreeMap<>());
    }

    @Override
    public synchronized boolean putIfAbsent(
            final P partitionKey, final R rangeKey, final V specifier)
            throws BitvantageStoreException, InterruptedException {
        final boolean wasAbsent;
        if (partitionedMap.containsKey(partitionKey)) {
            final NavigableMap<R, V> partition
                    = partitionedMap.get(partitionKey);
            if (partition.containsKey(rangeKey)) {
                wasAbsent = false;
            } else {
                wasAbsent = true;
            }
        } else {
            wasAbsent = true;
        }
        if (wasAbsent) {
            put(partitionKey, rangeKey, specifier);
        }
        return wasAbsent;
    }

    @Override
    public V get(final P partition, final R rangeValue)
            throws BitvantageStoreException, InterruptedException {
        final NavigableMap<R, V> partitionValues
                = partitionedMap.get(partition);

        return (partitionValues == null) ? null
                : partitionValues.get(rangeValue);
    }

    @Override
    public boolean contains(P partition, R rangeValue) throws
            BitvantageStoreException, InterruptedException {
        return partitionedMap.containsKey(partition) &&
               partitionedMap.get(partition).containsKey(rangeValue);
    }

}
