package org.example.processors.lm;

import org.apache.nifi.serialization.record.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class RecordFieldConverterTest {

    @Test
    @DisplayName("Integer converter is present and can convert Strings")
    public void testConverter_Integer() {
        RecordFieldConverter c = RecordFieldConverter.converter(Integer.class);
        assertNotNull(c);
        assertEquals(15, c.convert(mock(DataType.class), "15"));
    }

    @Test
    @DisplayName("Long converter is present and can convert Strings")
    public void testConverter_Long() {
        RecordFieldConverter c = RecordFieldConverter.converter(Long.class);
        assertNotNull(c);
        assertEquals(15L, c.convert(mock(DataType.class), "15"));
    }

    @Test
    @DisplayName("LocalDate converter is present and can convert ISO8601 Strings")
    public void testConverter_LocalDate() {
        RecordFieldConverter c = RecordFieldConverter.converter(LocalDate.class);
        assertNotNull(c);
        assertEquals(LocalDate.of(2020, 9, 1), c.convert(mock(DataType.class), "2020-09-01"));
    }

    @Test
    @DisplayName("LocalDateTime converter is present and can convert ISO8601 Strings")
    public void testConverter_LocalDateTime() {
        RecordFieldConverter c = RecordFieldConverter.converter(LocalDateTime.class);
        assertNotNull(c);
        assertEquals(
                LocalDateTime.of(2020, 9, 1, 11, 5, 6),
                c.convert(mock(DataType.class), "2020-09-01T11:05:06"));
    }

    @Test
    @DisplayName("For unknown type converter should return original String")
    public void testConverter_CustomType() {
        RecordFieldConverter c = RecordFieldConverter.converter(MyObject.class);
        assertNotNull(c);
        assertEquals("test", c.convert(mock(DataType.class), "test"));
    }

    public static class MyObject {

    }
}
