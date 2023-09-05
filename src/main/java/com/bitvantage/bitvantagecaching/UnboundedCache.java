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

import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class UnboundedCache<K extends PartitionKey, V> implements TwoLevelCache<K, V> {

    private final Store<K, V> store;
    private int hits = 0;
    private int misses = 0;
    private int puts = 0;

    @Override
    public V get(K key) throws InterruptedException, BitvantageStoreException {
        V value = store.get(key);
        if (value == null) {
            misses++;
        } else {
            hits++;
        }
        return value;
    }

    @Override
    public void put(K key, V value) throws InterruptedException,
            BitvantageStoreException {
        puts++;
        store.put(key, value);
    }

  @Override
  public void invalidate(K key) throws InterruptedException, BitvantageStoreException {
    store.delete(key);
  }

}
