package org.example.processors.lm.util;

import com.nhl.dflib.*;
import com.nhl.dflib.jdbc.connector.metadata.DbTableMetadata;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;

public class RecordsToDataFrameConverter {

    private ComponentLog logger;

    public RecordsToDataFrameConverter(ComponentLog logger) {
        this.logger = logger;
    }

    public DataFrame toDataFrame(RecordReader recordReader, DbTableMetadata tableMetadata)
            throws MalformedRecordException, IOException {

        RecordSchema schema = recordReader.getSchema();

        // the purpose of value conversion is to "normalize" column Java types against the target DB, so that we could
        // compare them in memory and avoid needless UPDATEs

        ConvertableRecordField[] converters = ConvertableRecordField.createFieldConverters(schema, tableMetadata, logger);

        // TODO: use primitive columns for primitive record fields to save memory
        Object[] row = new Object[converters.length];
        DataFrameByRowBuilder builder = DataFrame.newFrame(ConvertableRecordField.createIndex(converters)).byRow();
        Record r;
        while ((r = recordReader.nextRecord()) != null) {

            for (int i = 0; i < converters.length; i++) {
                row[i] = converters[i].convert(r);
            }

            builder.addRow(row);
        }

        return builder.create();
    }
}
