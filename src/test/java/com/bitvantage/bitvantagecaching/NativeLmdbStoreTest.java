/*
 * Copyright 2018 Public Transit Analytics.
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

import com.bitvantage.bitvantagecaching.testhelpers.TestKey;
import com.google.common.io.Files;
import java.io.File;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Public Transit Analytics
 */
public class NativeLmdbStoreTest {
     @Test
    public void testDoesNotContainKey() throws Exception {
        final NativeLmdbStore<TestKey, String> store = getEmptyStore();
        Assert.assertFalse(store.containsKey(new TestKey("any")));
    }

    @Test
    public void testContainsKey() throws Exception {
        final NativeLmdbStore<TestKey, String> store = getEmptyStore();
        final TestKey key = new TestKey("key");
        store.put(key, "");
        Assert.assertTrue(store.containsKey(key));
    }

    private NativeLmdbStore<TestKey, String> getEmptyStore() {
        final File storeDir = Files.createTempDir();
        final Path path = storeDir.toPath();

        final NativeLmdbStore<TestKey, String> store = new NativeLmdbStore<>(
                path, new GsonSerializer(String.class), 1);

        return store;
    }
   
}