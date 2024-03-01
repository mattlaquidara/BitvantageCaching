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

import com.google.common.collect.RangeMap;
import java.util.SortedMap;

public interface RangedCache<P extends PartitionKey, R extends RangeKey<R>, V> {

    public RangeMap<R, RangeStatus<R, V>> getRange(P partition, R min, R max)
            throws InterruptedException, BitvantageStoreException;

    public void putRange(P partition, R requestedMin, R requestedMax,
                         SortedMap<R, V> values)
            throws InterruptedException, BitvantageStoreException;

    public void put(P partition, R range, V value)
            throws InterruptedException, BitvantageStoreException;
    
    public void invalidate(P partition)
            throws InterruptedException, BitvantageStoreException;

}
