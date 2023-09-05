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

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

public class UnboundedRangedCache<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedCache<P, R, V> {

    private final Map<P, TreeRangeSet<R>> partitionedRequestedRanges;
    private final RangedStore<P, R, V> store;

    public UnboundedRangedCache(final RangedStore store) {
        this.store = store;
        partitionedRequestedRanges = new HashMap<>();
    }

    @Override
    public synchronized RangeMap<R, RangeStatus<R, V>> getRange(
            final P partition, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {
        final Range<R> requestRange = Range.closed(min, max);

        final TreeRangeSet<R> requestedRanges
                = partitionedRequestedRanges.getOrDefault(
                        partition, TreeRangeSet.create());

        final RangeMap<R, RangeStatus<R, V>> result;
        if (requestedRanges.encloses(requestRange)) {
            final SortedMap<R, V> values = ImmutableSortedMap.copyOf(
                    store.getValuesInRange(partition, min, max));
            result = ImmutableRangeMap.of(requestRange,
                                          new RangeStatus(true, values));
        } else {
            final RangeSet<R> cachedSubRanges;
            final RangeSet<R> uncachedSubRanges;

            cachedSubRanges = ImmutableRangeSet.copyOf(
                    requestedRanges.subRangeSet(requestRange));
            uncachedSubRanges = ImmutableRangeSet.copyOf(
                    requestedRanges.complement().subRangeSet(requestRange));

            final ImmutableRangeMap.Builder<R, RangeStatus<R, V>> rangeMapBuilder
                    = ImmutableRangeMap.builder();
            for (final Range<R> subRange : cachedSubRanges.asRanges()) {
                final SortedMap<R, V> values = ImmutableSortedMap.copyOf(
                        store.getValuesInRange(partition,
                                               subRange.lowerEndpoint(),
                                               subRange.upperEndpoint()));
                rangeMapBuilder.put(subRange, new RangeStatus(true, values));
            }
            for (final Range<R> subRange : uncachedSubRanges.asRanges()) {
                rangeMapBuilder.put(subRange, new RangeStatus(false, null));
            }
            result = rangeMapBuilder.build();
        }
        partitionedRequestedRanges.putIfAbsent(partition, requestedRanges);
        return result;
    }

    @Override
    public synchronized void putRange(final P partition, final R requestedMin,
                                      final R requestedMax,
                                      final SortedMap<R, V> values)
            throws InterruptedException, BitvantageStoreException {
        store.putAll(partition, values);
        final TreeRangeSet<R> requestedRanges
                = partitionedRequestedRanges.getOrDefault(
                        partition, TreeRangeSet.create());
        requestedRanges.add(Range.closed(requestedMin, requestedMax));
        partitionedRequestedRanges.putIfAbsent(partition, requestedRanges);

    }

    @Override
    public synchronized void put(final P partition, final R range,
                                 final V value)
            throws InterruptedException, BitvantageStoreException {
        store.put(partition, range, value);
        final TreeRangeSet<R> requestedRanges
                = partitionedRequestedRanges.getOrDefault(
                        partition, TreeRangeSet.create());
        requestedRanges.add(Range.closed(range, range));
        partitionedRequestedRanges.putIfAbsent(partition, requestedRanges);

    }

}
