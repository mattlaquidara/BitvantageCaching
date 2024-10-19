package com.bitvantage.bitvantagecaching.memory;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.Store;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryHashStore<K extends PartitionKey, V> implements Store<K, V> {

  private final Map<K, V> map;

  public InMemoryHashStore() {
    map = new ConcurrentHashMap<>();
  }

  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public Optional<V> get(K key) {
    return Optional.ofNullable(map.get(key));
  }

  @Override
  public void put(K key, V value) {
    map.put(key, value);
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public Map<K, V> getAll() {
    return ImmutableMap.copyOf(map);
  }

  @Override
  public void putAll(final Map<K, V> entries) {
    map.putAll(entries);
  }

  @Override
  public void delete(final K key) throws BitvantageStoreException, InterruptedException {
    map.remove(key);
  }
}
