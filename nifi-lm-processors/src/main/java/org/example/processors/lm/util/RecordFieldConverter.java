package org.example.processors.lm.util;

import org.apache.nifi.serialization.record.DataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

// Converter is not parameterized by the target type, as it may bypass conversion and return a String for unknown types,
// which may still work as PreparedStatement parameter with many DB's
public abstract class RecordFieldConverter {

    private static final Map<Class, RecordFieldConverter> cache;
    private static final RecordFieldConverter defaultConverter;

    static {
        defaultConverter = new NoopConverter();
        cache = new HashMap<>();
        cache.put(Integer.class, new IntegerConverter());
        cache.put(Long.class, new LongConverter());
        cache.put(LocalDate.class, new LocalDateConverter());
        cache.put(LocalDateTime.class, new LocalDateTimeConverter());
    }

    private static final class NoopConverter extends RecordFieldConverter {
        @Override
        protected Object convertNotNull(DataType recordType, String value) {
            return value;
        }
    }

    private static final class IntegerConverter extends RecordFieldConverter {
        @Override
        protected Integer convertNotNull(DataType recordType, String value) {
            return Integer.valueOf(value);
        }
    }

    private static final class LongConverter extends RecordFieldConverter {
        @Override
        protected Long convertNotNull(DataType recordType, String value) {
            return Long.valueOf(value);
        }
    }

    private static class LocalDateConverter extends RecordFieldConverter {
        @Override
        protected LocalDate convertNotNull(DataType recordType, String value) {
            // TODO: would DataType.getFormat() be of help here?
            return LocalDate.parse(value);
        }
    }

    private static class LocalDateTimeConverter extends RecordFieldConverter {
        @Override
        protected LocalDateTime convertNotNull(DataType recordType, String value) {
            // TODO: would DataType.getFormat() be of help here?
            return LocalDateTime.parse(value);
        }
    }

    public static RecordFieldConverter converter(Class<?> type) {
        return cache.getOrDefault(type, defaultConverter);
    }

    public Object convert(DataType recordType, String value) {
        return value == null ? null : convertNotNull(recordType, value);
    }

    protected abstract Object convertNotNull(DataType recordType, String value);
}
