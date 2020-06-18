package org.example.processors.lm;

import com.nhl.dflib.Index;
import com.nhl.dflib.jdbc.connector.metadata.DbColumnMetadata;
import com.nhl.dflib.jdbc.connector.metadata.DbTableMetadata;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata describing a single field
 */
public class ConvertableRecordField {

    private RecordField field;
    private RecordFieldConverter converter;

    public static Index createIndex(ConvertableRecordField[] fields) {
        String[] labels = new String[fields.length];

        for (int i = 0; i < labels.length; i++) {
            labels[i] = fields[i].getName();
        }

        return Index.forLabels(labels);
    }

    public static ConvertableRecordField[] createFieldConverters(
            RecordSchema schema,
            DbTableMetadata targetTable,
            ComponentLog logger) {

        List<String> names = schema.getFieldNames();
        List<ConvertableRecordField> fields = new ArrayList<>(names.size());

        for (int i = 0; i < names.size(); i++) {

            String name = names.get(i);
            if (!targetTable.hasColumn(name)) {
                logger.info("Skipping column '{}' that is not in the target table '{}'", new Object[]{name, targetTable.getName()});
                continue;
            }

            DbColumnMetadata targetColumn = targetTable.getColumn(name);
            RecordFieldConverter converter = RecordFieldConverter.converter(typeForJdbcType(targetColumn.getType()));
            ConvertableRecordField field = new ConvertableRecordField(schema.getField(name).get(), converter);
            fields.add(field);
        }

        return fields.toArray(new ConvertableRecordField[0]);
    }

    private static Class<?> typeForJdbcType(int jdbcType) {

        switch (jdbcType) {
            case Types.BIGINT:
                return Long.class;
            case Types.INTEGER:
                return Integer.class;
            case Types.DATE:
                return LocalDate.class;
            case Types.TIMESTAMP:
                return LocalDateTime.class;
            default:
                return String.class;
        }
    }

    public ConvertableRecordField(RecordField field, RecordFieldConverter converter) {
        this.field = field;
        this.converter = converter;
    }

    public Object convert(Record record) {
        return converter.convert(field.getDataType(), record.getAsString(field.getFieldName()));
    }

    public String getName() {
        return field.getFieldName();
    }
}
