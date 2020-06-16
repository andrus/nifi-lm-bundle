package org.example.processors.lm;

import com.nhl.dflib.*;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;

public class DFLibConverter {

    static DataFrame toDataFrame(RecordReader recordReader) throws MalformedRecordException, IOException {
        RecordSchema schema = recordReader.getSchema();
        Index columns = Index.forLabels(Series.forData(schema.getFieldNames()));

        // TODO: use primitive columns for primitive record fields
        DataFrameByRowBuilder builder = DataFrame.newFrame(columns).byRow();
        Record r;
        while ((r = recordReader.nextRecord()) != null) {
            builder.addRow(r.getValues());
        }

        return builder.create();
    }
}
