package org.example.processors.lm.util;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Printers;
import com.nhl.dflib.Series;
import com.nhl.dflib.jdbc.Jdbc;
import com.nhl.dflib.jdbc.SaveOp;
import com.nhl.dflib.jdbc.connector.JdbcConnector;
import com.nhl.dflib.jdbc.connector.SaveStats;
import com.nhl.dflib.jdbc.connector.TableSaver;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.example.processors.lm.UpsertSQL;

import java.io.IOException;
import java.util.Objects;

public class UpsertBuilder {

    private ComponentLog logger;
    private DBCPService db;
    private MatchStrategy matchStrategy;
    private String targetTable;
    private String[] keyColumns;

    public static UpsertBuilder create(ComponentLog logger) {
        return new UpsertBuilder(logger);
    }

    protected UpsertBuilder(ComponentLog logger) {
        this.logger = Objects.requireNonNull(logger);
    }

    public UpsertBuilder db(DBCPService db) {
        this.db = db;
        return this;
    }

    public UpsertBuilder matchStrategy(String name) {
        this.matchStrategy = MatchStrategy.valueOf(name);
        return this;
    }

    public UpsertBuilder targetTable(String targetTable) {
        this.targetTable = targetTable;
        return this;
    }

    public UpsertBuilder keyColumns(String keyColumns) {

        if (keyColumns == null) {
            this.keyColumns = null;
        } else {
            this.keyColumns = keyColumns.split(",");
        }

        return this;
    }

    public void upsert(RecordReader reader) throws IOException, MalformedRecordException {
        JdbcConnector connector = createConnector();

        // TODO: validate all properties are set
        DataFrame df = new RecordsToDataFrameConverter(logger).toDataFrame(reader, connector.getMetadata().getTable(targetTable));
        SaveStats saveStats = createSaver(connector).save(df);
        Series<SaveOp> rowStatuses = saveStats.getRowSaveStatuses();

        if (rowStatuses.size() > 0) {
            logger.info(Printers.inline.toString(rowStatuses.valueCounts()));
        }

        // TODO: pass classified flow file down the pipe
    }

    protected JdbcConnector createConnector() {
        return Jdbc.connector(new DBCPServiceDataSource(db));
    }

    protected TableSaver createSaver(JdbcConnector connector) {
        TableSaver saver = connector.tableSaver(targetTable);

        switch (matchStrategy) {
            case pk:
                return saver.mergeByPk();
            case key_columns:
                if (keyColumns == null || keyColumns.length == 0) {
                    throw new ProcessException("No '" + UpsertSQL.KEY_COLUMNS_PROPERTY.getName() +
                            "' property set. It is required by 'key_columns' row matching strategy");
                }
                return saver.mergeByColumns(keyColumns);
            case insert_only:
            default:
                return saver;
        }
    }
}
