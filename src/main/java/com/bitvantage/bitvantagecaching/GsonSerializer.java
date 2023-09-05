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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.nio.charset.StandardCharsets;

public class GsonSerializer<V> implements ValueSerializer<V> {

    private final Gson gson;
    private final TypeToken typeToken;

    public GsonSerializer(final Class<V> valueClass) {
        gson = new Gson();
        typeToken = TypeToken.get(valueClass);
    }

    public GsonSerializer(final TypeToken typeToken) {
        gson = new Gson();
        this.typeToken = typeToken;
    }

    @Override
    public byte[] getBytes(final V value) {
        final String json = gson.toJson(value);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public V getValue(byte[] bytes) {
        final String json = new String(bytes, StandardCharsets.UTF_8);
        return gson.fromJson(json, typeToken.getType());
    }

}
