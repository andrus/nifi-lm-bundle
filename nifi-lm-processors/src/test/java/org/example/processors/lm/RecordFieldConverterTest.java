package org.example.processors.lm;

import org.apache.nifi.serialization.record.DataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class RecordFieldConverterTest {

    @Test
    @DisplayName("LocalDateTime converter is present and can convert ISO8601 Strings")
    public void testConverter_LocalDateTime() {
        RecordFieldConverter<LocalDateTime> c = RecordFieldConverter.converter(LocalDateTime.class);
        assertNotNull(c);
        assertEquals(
                LocalDateTime.of(2020, 9, 1, 11, 5, 6),
                c.convert(mock(DataType.class), "2020-09-01T11:05:06"));
    }
}
