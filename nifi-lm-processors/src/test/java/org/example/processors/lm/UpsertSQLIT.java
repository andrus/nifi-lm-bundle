package org.example.processors.lm;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.jdbc.Jdbc;
import com.nhl.dflib.junit5.DataFrameAsserts;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

@Testcontainers
public class UpsertSQLIT {

    @Container
    static final PostgreSQLContainer db = new PostgreSQLContainer("postgres:11");

    private static final String SOURCE_READER = "SourceReader";
    private static final String TARGET_POOL = "TargetPool";

    static final DBCPService targetPool = new DBCPServiceImpl();
    static final Schema avroSchema;

    static {
        try {
            avroSchema = new Schema.Parser().parse(UpsertSQLIT.class.getResourceAsStream("test.avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    static void initTargetSchema() throws SQLException {
        try (Connection c = targetPool.getConnection()) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate("create table test_table (id bigint primary key, name varchar(100))");
            }
        }
    }

    @BeforeEach
    void deleteTableData() throws SQLException {
        try (Connection c = targetPool.getConnection()) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate("delete from test_table");
            }
        }
    }

    private byte[] encodeAsAvro(GenericRecord... data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.create(avroSchema, out);
            for (GenericRecord r : data) {
                writer.append(r);
            }
        }

        return out.toByteArray();
    }

    private GenericRecord createRecord(long id, String name) {
        GenericRecord r = new GenericData.Record(avroSchema);
        r.put("id", id);
        r.put("name", name);
        return r;
    }

    private DataFrameAsserts assertDbData() {
        DataFrame df = Jdbc.connector(new DBCPServiceDataSource(targetPool))
                .tableLoader("test_table")
                .load()
                .sort("id", true);
        return new DataFrameAsserts(df, "id", "name");
    }

    private TestRunner createRunnerWithControllers() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(UpsertSQL.class);

        RecordReaderFactory sourceReader = new AvroReader();
        runner.addControllerService(SOURCE_READER, sourceReader);
        runner.assertValid(sourceReader);
        runner.enableControllerService(sourceReader);

        runner.addControllerService(TARGET_POOL, targetPool, new HashMap<>());
        runner.assertValid(targetPool);
        runner.enableControllerService(targetPool);

        runner.assertNotValid();

        return runner;
    }

    @Test
    @DisplayName("Minimal configuration")
    public void testMinConfig() throws InitializationException {

        TestRunner runner = createRunnerWithControllers();

        runner.setProperty(UpsertSQL.SOURCE_RECORD_READER, SOURCE_READER);
        runner.assertNotValid();

        runner.setProperty(UpsertSQL.TARGET_CONNECTION_POOL_PROPERTY, TARGET_POOL);
        runner.assertNotValid();

        runner.setProperty(UpsertSQL.TABLE_TABLE_NAME_PROPERTY, "test_table");
        runner.assertValid();
    }

    @Test
    @DisplayName("By columns configuration")
    public void testByColumnsConfig() throws InitializationException {

        TestRunner runner = createRunnerWithControllers();

        runner.setProperty(UpsertSQL.SOURCE_RECORD_READER, SOURCE_READER);
        runner.setProperty(UpsertSQL.TARGET_CONNECTION_POOL_PROPERTY, TARGET_POOL);
        runner.setProperty(UpsertSQL.TABLE_TABLE_NAME_PROPERTY, "test_table");

        runner.setProperty(UpsertSQL.MATCH_STRATEGY_PROPERTY, MatchStrategy.key_columns.name());
        runner.assertNotValid();

        runner.setProperty(UpsertSQL.KEY_COLUMNS_PROPERTY, "name,id");
        runner.assertValid();
    }

    @Test
    @DisplayName("Upsert matching by PK")
    public void testByPk() throws InitializationException, IOException {
        TestRunner runner = createRunnerWithControllers();
        runner.setProperty(UpsertSQL.SOURCE_RECORD_READER, SOURCE_READER);
        runner.setProperty(UpsertSQL.TARGET_CONNECTION_POOL_PROPERTY, TARGET_POOL);
        runner.setProperty(UpsertSQL.TABLE_TABLE_NAME_PROPERTY, "test_table");

        runner.enqueue(encodeAsAvro(createRecord(1L, "a"), createRecord(2L, "b")));
        runner.run();
        runner.assertTransferCount(UpsertSQL.SUCCESS_RELATIONSHIP, 1);
        runner.assertTransferCount(UpsertSQL.FAILURE_RELATIONSHIP, 0);
        assertDbData().expectHeight(2)
                .expectRow(0, 1L, "a")
                .expectRow(1, 2L, "b");

        runner.enqueue(encodeAsAvro(createRecord(1L, "a"), createRecord(2L, "c"), createRecord(3L, "d")));
        runner.run();
        runner.assertTransferCount(UpsertSQL.SUCCESS_RELATIONSHIP, 2);
        runner.assertTransferCount(UpsertSQL.FAILURE_RELATIONSHIP, 0);
        assertDbData().expectHeight(3)
                .expectRow(0, 1L, "a")
                .expectRow(1, 2L, "c")
                .expectRow(2, 3L, "d");
    }

    @Test
    @DisplayName("Upsert matching by columns")
    public void testByColumns() throws InitializationException, IOException {
        TestRunner runner = createRunnerWithControllers();
        runner.setProperty(UpsertSQL.SOURCE_RECORD_READER, SOURCE_READER);
        runner.setProperty(UpsertSQL.TARGET_CONNECTION_POOL_PROPERTY, TARGET_POOL);
        runner.setProperty(UpsertSQL.TABLE_TABLE_NAME_PROPERTY, "test_table");
        runner.setProperty(UpsertSQL.MATCH_STRATEGY_PROPERTY, MatchStrategy.key_columns.name());
        runner.setProperty(UpsertSQL.KEY_COLUMNS_PROPERTY, "name");

        runner.enqueue(encodeAsAvro(createRecord(1L, "a"), createRecord(2L, "b")));
        runner.run();
        runner.assertTransferCount(UpsertSQL.SUCCESS_RELATIONSHIP, 1);
        runner.assertTransferCount(UpsertSQL.FAILURE_RELATIONSHIP, 0);
        assertDbData().expectHeight(2)
                .expectRow(0, 1L, "a")
                .expectRow(1, 2L, "b");

        runner.enqueue(encodeAsAvro(createRecord(3L, "a"), createRecord(4L, "c")));
        runner.run();
        runner.assertTransferCount(UpsertSQL.SUCCESS_RELATIONSHIP, 2);
        runner.assertTransferCount(UpsertSQL.FAILURE_RELATIONSHIP, 0);
        assertDbData().expectHeight(3)
                .expectRow(0, 2L, "b")
                .expectRow(1, 3L, "a")
                .expectRow(2, 4L, "c");
    }

    private static class DBCPServiceImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return TARGET_POOL;
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                return DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
            } catch (Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}
