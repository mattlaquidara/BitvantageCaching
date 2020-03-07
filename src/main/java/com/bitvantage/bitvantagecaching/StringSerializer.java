/*
 * Copyright 2018 Matt Laquidara.
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

import java.nio.charset.StandardCharsets;

/**
 * Serializes a string by representing it in UTF-8 byte representation.
 * @author Matt Laquidara
 */
public class StringSerializer implements ValueSerializer<String> {

    @Override
    public byte[] getBytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getValue(final byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
}
