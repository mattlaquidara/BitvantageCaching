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
package com.bitvantage.bitvantagecaching.dynamo;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface DynamoStoreSerializer<P extends PartitionKey, V> {

    byte[] getPartitionKey(P key) throws BitvantageStoreException;

    String getPartitionKeyName() throws BitvantageStoreException;

    Map<String,AttributeValue> serialize(P partition, V value) throws BitvantageStoreException;

    V deserializeValue(Map<String,AttributeValue> item) throws BitvantageStoreException;

    P deserializeKey(Map<String,AttributeValue> item) throws BitvantageStoreException;

}
