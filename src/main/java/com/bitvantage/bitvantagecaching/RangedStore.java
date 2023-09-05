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
import java.util.NavigableMap;

public interface RangedStore<P extends PartitionKey, R extends RangeKey<R>, V> {

    NavigableMap<R, V> getValuesInRange(P partition, R min, R max)
            throws InterruptedException, BitvantageStoreException;

    NavigableMap<R, V> getValuesAbove(P partition, R min)
            throws InterruptedException, BitvantageStoreException;

    NavigableMap<R, V> getValuesBelow(P partition, R max)
            throws InterruptedException, BitvantageStoreException;

    NavigableMap<R, V> getNextValues(P partition, R min, int count)
            throws InterruptedException, BitvantageStoreException;

    NavigableMap<R, V> getHeadValues(P partition, int count)
            throws InterruptedException, BitvantageStoreException;

    NavigableMap<R, V> getPartition(P partition)
            throws InterruptedException, BitvantageStoreException;

    boolean contains(P partition, R rangeValue)
            throws BitvantageStoreException, InterruptedException;
    
    V get(P partition, R rangeValue)
            throws BitvantageStoreException, InterruptedException;

    void put(P partition, R rangeValue, V value)
            throws BitvantageStoreException, InterruptedException;

    void putAll(P partition, Map<R, V> entries)
            throws BitvantageStoreException, InterruptedException;

    boolean isEmpty() throws BitvantageStoreException, InterruptedException;

}
