/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.greptimedb.dao;

import io.greptime.models.DataType;
import org.apache.skywalking.oap.server.core.storage.type.StorageDataComplexObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeDBTableBuilderTest {

    // ---- coerceValue: null handling ----

    @Test
    void coerceValueShouldReturnNullForNullInput() {
        assertNull(GreptimeDBTableBuilder.coerceValue(null, DataType.String));
        assertNull(GreptimeDBTableBuilder.coerceValue(null, DataType.Int32));
        assertNull(GreptimeDBTableBuilder.coerceValue(null, DataType.Int64));
        assertNull(GreptimeDBTableBuilder.coerceValue(null, DataType.Float64));
    }

    // ---- coerceValue: String type ----

    @Test
    void coerceValueShouldPassthroughString() {
        final Object result = GreptimeDBTableBuilder.coerceValue("hello", DataType.String);
        assertEquals("hello", result);
    }

    @Test
    void coerceValueShouldConvertNonStringToStringViaToString() {
        final Object result = GreptimeDBTableBuilder.coerceValue(42, DataType.String);
        assertEquals("42", result);
    }

    // ---- coerceValue: Int32 type ----

    @Test
    void coerceValueShouldPassthroughInteger() {
        final Object result = GreptimeDBTableBuilder.coerceValue(42, DataType.Int32);
        assertTrue(result instanceof Integer);
        assertEquals(42, result);
    }

    @Test
    void coerceValueShouldConvertLongToIntForInt32() {
        final Object result = GreptimeDBTableBuilder.coerceValue(42L, DataType.Int32);
        assertTrue(result instanceof Integer);
        assertEquals(42, result);
    }

    @Test
    void coerceValueShouldConvertDoubleToIntForInt32() {
        final Object result = GreptimeDBTableBuilder.coerceValue(3.14, DataType.Int32);
        assertTrue(result instanceof Integer);
        assertEquals(3, result);
    }

    // ---- coerceValue: Int64 type ----

    @Test
    void coerceValueShouldPassthroughLongForInt64() {
        final Object result = GreptimeDBTableBuilder.coerceValue(100L, DataType.Int64);
        assertEquals(100L, result);
    }

    @Test
    void coerceValueShouldPassthroughIntegerForInt64() {
        // SDK's ValueUtil.getLongValue() handles Integer->Long conversion internally
        final Object result = GreptimeDBTableBuilder.coerceValue(42, DataType.Int64);
        assertEquals(42, result);
    }

    // ---- coerceValue: Float64 type ----

    @Test
    void coerceValueShouldPassthroughDouble() {
        final Object result = GreptimeDBTableBuilder.coerceValue(3.14, DataType.Float64);
        assertTrue(result instanceof Double);
        assertEquals(3.14, result);
    }

    @Test
    void coerceValueShouldConvertIntegerToDoubleForFloat64() {
        final Object result = GreptimeDBTableBuilder.coerceValue(42, DataType.Float64);
        assertTrue(result instanceof Double);
        assertEquals(42.0, result);
    }

    @Test
    void coerceValueShouldConvertFloatToDoubleForFloat64() {
        final Object result = GreptimeDBTableBuilder.coerceValue(1.5f, DataType.Float64);
        assertTrue(result instanceof Double);
        assertEquals(1.5, (Double) result, 0.0001);
    }

    // ---- coerceValue: Float32 type ----

    @Test
    void coerceValueShouldPassthroughFloat() {
        final Object result = GreptimeDBTableBuilder.coerceValue(1.5f, DataType.Float32);
        assertTrue(result instanceof Float);
        assertEquals(1.5f, result);
    }

    @Test
    void coerceValueShouldConvertDoubleToFloatForFloat32() {
        final Object result = GreptimeDBTableBuilder.coerceValue(1.5, DataType.Float32);
        assertTrue(result instanceof Float);
        assertEquals(1.5f, result);
    }

    @Test
    void coerceValueShouldConvertIntegerToFloatForFloat32() {
        final Object result = GreptimeDBTableBuilder.coerceValue(42, DataType.Float32);
        assertTrue(result instanceof Float);
        assertEquals(42.0f, result);
    }

    // ---- coerceValue: StorageDataComplexObject ----

    @Test
    void coerceValueShouldCallToStorageDataForComplexObject() {
        final StorageDataComplexObject<String> complexObj = new StorageDataComplexObject<String>() {
            @Override
            public String toStorageData() {
                return "{\"key\":\"value\"}";
            }

            @Override
            public void toObject(final String data) {
            }

            @Override
            public void copyFrom(final String source) {
            }
        };
        final Object result = GreptimeDBTableBuilder.coerceValue(complexObj, DataType.String);
        assertEquals("{\"key\":\"value\"}", result);
    }

    // ---- coerceValue: default/unknown type passthrough ----

    @Test
    void coerceValueShouldPassthroughForBinaryType() {
        final byte[] data = new byte[]{1, 2, 3};
        final Object result = GreptimeDBTableBuilder.coerceValue(data, DataType.Binary);
        assertEquals(data, result);
    }
}
