/*
 * Copyright 2019 Matt Laquidara.
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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LongKey implements RangeKey<LongKey> {

    private static final LongKey MIN
            = new LongKey(0);
    private static final LongKey MAX
            = new LongKey(0xFFFFFFFFFFFFFFFFL);

    @Getter
    private final long value;

    @Override
    public LongKey getRangeMin() {
        return MIN;
    }

    @Override
    public LongKey getRangeMax() {
        return MAX;
    }

    @Override
    public int compareTo(final LongKey o) {
        return Long.compare(value, o.value);
    }

}
