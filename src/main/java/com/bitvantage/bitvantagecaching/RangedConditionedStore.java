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

public interface RangedConditionedStore<P extends PartitionKey, R extends RangeKey<R>, V>

        extends RangedStore<P, R, V> {
    
     /**
     * Conditionally put an item in the store.
     *
     * @param partition The partition in which to place the item.
     * @param range The range within the partition.
     * @param value The value to put in the store.
     * @return true if the call did put, false if an item was present.
     * @throws BitvantageStoreException
     * @throws InterruptedException
     */
    public boolean putIfAbsent(P partition, R range, V value)
            throws BitvantageStoreException, InterruptedException;

}
