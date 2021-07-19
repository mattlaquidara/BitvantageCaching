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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 * @author Matt Laquidara
 */
public class TwoLevelCachingRangedStore<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedConditionedStore<P, R, V> {

    private final RangedConditionedStore<P, R, V> store;
    private final RangedCache<P, R, V> cache;

    public TwoLevelCachingRangedStore(
            final RangedConditionedStore<P, R, V> store,
            final RangedCache<P, R, V> cache) {
        this.store = store;
        this.cache = cache;
    }

    @Override
    public NavigableMap<R, V> getValuesInRange(
            final P partition, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {
        final RangeMap<R, RangeStatus<R, V>> response
                = cache.getRange(partition, min, max);

        final ImmutableSortedMap.Builder<R, V> responseBuilder
                = new ImmutableSortedMap.Builder(Ordering.natural());

        for (final Map.Entry<Range<R>, RangeStatus<R, V>> entry : response
                .asMapOfRanges().entrySet()) {
            if (entry.getValue().isCached()) {
                responseBuilder.putAll(entry.getValue().getValues());
            } else {
                final NavigableMap<R, V> values = store.getValuesInRange(
                        partition, entry.getKey().lowerEndpoint(),
                        entry.getKey().upperEndpoint());

                responseBuilder.putAll(values);
                cache.putRange(partition, min, max, values);
            }
        }

        return responseBuilder.build();
    }

    @Override
    public NavigableMap<R, V> getValuesAbove(final P partition, final R min)
            throws InterruptedException, BitvantageStoreException {
        final R max = min.getRangeMax();
        return getValuesInRange(partition, min, max);
    }

    @Override
    public NavigableMap<R, V> getValuesBelow(final P partition, final R max)
            throws InterruptedException, BitvantageStoreException {

        return getValuesInRange(partition, max.getRangeMin(), max);
    }

    @Override
    public NavigableMap<R, V> getNextValues(final P partition, final R min,
                                            final int count)
            throws InterruptedException, BitvantageStoreException {
        return store.getNextValues(partition, min, count);
    }

    @Override
    public NavigableMap<R, V> getHeadValues(final P partition, final int count)
            throws InterruptedException, BitvantageStoreException {
        return store.getHeadValues(partition, count);
    }

    @Override
    public void put(final P partition, final R rangeValue, final V value)
            throws BitvantageStoreException, InterruptedException {
        store.put(partition, rangeValue, value);
    }

    @Override
    public void putAll(final P partition, final Map<R, V> entries)
            throws BitvantageStoreException, InterruptedException {
        store.putAll(partition, entries);
    }

    @Override
    public boolean isEmpty() throws BitvantageStoreException,
            InterruptedException {
        return store.isEmpty();
    }

    @Override
    public NavigableMap<R, V> getPartition(final P partition) throws
            InterruptedException, BitvantageStoreException {
        return store.getPartition(partition);
    }

    @Override
    public boolean putIfAbsent(P partitionKey, R rangeKey, V specifier)
            throws BitvantageStoreException, InterruptedException {
        return store.putIfAbsent(partitionKey, rangeKey, specifier);
    }

    @Override
    public V get(final P partition, final R rangeValue)
            throws BitvantageStoreException, InterruptedException {
        final NavigableMap<R, V> values = getValuesInRange(
                partition, rangeValue, rangeValue);
        if (values.isEmpty()) {
            return null;
        }
        return values.values().iterator().next();
    }

    @Override
    public boolean contains(P partition, R rangeValue) throws
            BitvantageStoreException, InterruptedException {
        return !getValuesInRange(partition, rangeValue, rangeValue).isEmpty();
    }

}
