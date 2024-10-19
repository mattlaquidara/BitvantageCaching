package com.bitvantage.bitvantagecaching;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class EmptyStore<K extends PartitionKey, V> implements Store<K, V> {

  @Override
  public boolean containsKey(K key) throws BitvantageStoreException, InterruptedException {
    return false;
  }

  @Override
  public Optional<V> get(K key) throws BitvantageStoreException, InterruptedException {
    return Optional.empty();
  }

  @Override
  public void put(K key, V value) throws BitvantageStoreException, InterruptedException {
  }

  @Override
  public void putAll(Map<K, V> entries) throws BitvantageStoreException, InterruptedException {
  }

  @Override
  public Map<K, V> getAll() throws BitvantageStoreException, InterruptedException {
    return Collections.emptyMap();
  }

  @Override
  public boolean isEmpty() throws BitvantageStoreException, InterruptedException {
    return true;
  }

  @Override
  public void delete(K key) throws BitvantageStoreException, InterruptedException {
  }

}
