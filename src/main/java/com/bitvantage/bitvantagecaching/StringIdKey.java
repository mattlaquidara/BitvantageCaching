package com.bitvantage.bitvantagecaching;

import lombok.Value;

@Value
public class StringIdKey implements PartitionKey {
  private final String id;
}
