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

import java.util.UUID;
import lombok.Value;

@Value
public class UuidKey implements PartitionKey, RangeKey<UuidKey> {
  private static final UuidKey MIN = new UuidKey(new UUID(0, 0));
  private static final UuidKey MAX = new UuidKey(new UUID(0xFFFFFFFFL, 0xFFFFFFFFL));
  private final UUID uuid;

  @Override
  public UuidKey getRangeMin() {
    return MIN;
  }

  @Override
  public UuidKey getRangeMax() {
    return MAX;
  }

  @Override
  public int compareTo(final UuidKey o) {
    return uuid.compareTo(o.uuid);
  }
}
