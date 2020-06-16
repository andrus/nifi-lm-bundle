package org.example.processors.lm;

import org.apache.nifi.serialization.record.DataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public abstract class RecordFieldConverter<T> {

    private static final Map<Class, RecordFieldConverter> cache;
    private static final RecordFieldConverter<Object> defaultConverter;

    static {
        defaultConverter = new NoopConverter();
        cache = new HashMap<>();
        cache.put(Integer.class, new IntegerConverter());
        cache.put(Long.class, new LongConverter());
        cache.put(LocalDate.class, new LocalDateConverter());
        cache.put(LocalDateTime.class, new LocalDateTimeConverter());
    }

    private static final class NoopConverter extends RecordFieldConverter<Object> {
        @Override
        protected Object convertNotNull(DataType recordType, String value) {
            return value;
        }
    }

    private static final class IntegerConverter extends RecordFieldConverter<Integer> {
        @Override
        protected Integer convertNotNull(DataType recordType, String value) {
            return Integer.valueOf(value);
        }
    }

    private static final class LongConverter extends RecordFieldConverter<Long> {
        @Override
        protected Long convertNotNull(DataType recordType, String value) {
            return Long.valueOf(value);
        }
    }

    private static class LocalDateConverter extends RecordFieldConverter<LocalDate> {
        @Override
        protected LocalDate convertNotNull(DataType recordType, String value) {
            // TODO: would DataType.getFormat() be of help here?
            return LocalDate.parse(value);
        }
    }

    private static class LocalDateTimeConverter extends RecordFieldConverter<LocalDateTime> {
        @Override
        protected LocalDateTime convertNotNull(DataType recordType, String value) {
            // TODO: would DataType.getFormat() be of help here?
            return LocalDateTime.parse(value);
        }
    }

    public static <T> RecordFieldConverter<T> converter(Class<T> type) {
        return cache.getOrDefault(type, defaultConverter);
    }

    public T convert(DataType recordType, String value) {
        return value == null ? null : convertNotNull(recordType, value);
    }

    protected abstract T convertNotNull(DataType recordType, String value);
}
