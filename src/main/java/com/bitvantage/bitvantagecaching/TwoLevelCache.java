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

/**
 * Interface for a key-value cache.
 *
 * @author Matt Laquidara
 */
public interface TwoLevelCache<P extends PartitionKey, V> {

    V get(P key) throws InterruptedException, BitvantageStoreException;

    void put(P key, V value) throws InterruptedException,
            BitvantageStoreException;
    
    void invalidate(P key) throws InterruptedException,
            BitvantageStoreException;
    
}
