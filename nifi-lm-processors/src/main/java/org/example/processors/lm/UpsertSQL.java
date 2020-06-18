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
 */
package org.example.processors.lm;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.example.processors.lm.util.MatchStrategy;
import org.example.processors.lm.util.UpsertBuilder;

import java.io.InputStream;
import java.util.*;

import static java.util.Arrays.asList;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"etl", "sql", "link-move"})
@CapabilityDescription("Loads FlowFile data to a DB table. Rows missing in DB are inserted, rows already in DB are updated.")
public class UpsertSQL extends AbstractProcessor {

    public static final PropertyDescriptor SOURCE_RECORD_READER = new PropertyDescriptor.Builder()
            .name("source-record-reader")
            .displayName("Source record reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TARGET_CONNECTION_POOL_PROPERTY = new PropertyDescriptor.Builder()
            .name("target-connection-pool")
            .displayName("Target DB connection pool")
            .description("The Controller Service that is used to obtain connection to the target database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TARGET_TABLE_NAME_PROPERTY = new PropertyDescriptor.Builder()
            .name("target-table-name")
            .displayName("Target table name")
            .description("The name of the target table for to insert or update data")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MATCH_STRATEGY_PROPERTY = new PropertyDescriptor.Builder()
            .name("match-strategy")
            .displayName("Row matching strategy")
            .description("How should we identify existing rows to be updated")
            .required(false)
            .allowableValues(MatchStrategy.values())
            .defaultValue(MatchStrategy.pk.name())
            .addValidator(UpsertSQL::customValidateMatchStrategy)
            .build();

    public static final PropertyDescriptor KEY_COLUMNS_PROPERTY = new PropertyDescriptor.Builder()
            .name("key-columns")
            .displayName("Key columns")
            .description("A comma-separated list of the column names in the target table to use for row matching. " +
                    "Ignored unless 'Row matching strategy' is 'key_columns'")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("TODO: a FlowFile with update stats")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason, the original FlowFile will "
                    + "be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    static ValidationResult customValidateMatchStrategy(String subject, String input, ValidationContext context) {
        if (MatchStrategy.key_columns.name().equals(input)) {
            String keyColumns = context.getProperties().get(KEY_COLUMNS_PROPERTY);
            return StandardValidators.NON_BLANK_VALIDATOR.validate(subject, keyColumns, context);
        } else {
            return Validator.VALID.validate(subject, input, context);
        }
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        this.descriptors = Collections.unmodifiableList(asList(
                SOURCE_RECORD_READER,
                TARGET_CONNECTION_POOL_PROPERTY,
                TARGET_TABLE_NAME_PROPERTY,
                MATCH_STRATEGY_PROPERTY,
                KEY_COLUMNS_PROPERTY));

        this.relationships = Collections.unmodifiableSet(new HashSet<>(asList(
                SUCCESS_RELATIONSHIP,
                FAILURE_RELATIONSHIP)));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile originalFF = session.get();
        if (originalFF == null) {
            return;
        }

        RecordReaderFactory srcReaderFactory = context
                .getProperty(SOURCE_RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        UpsertBuilder upserter = UpsertBuilder
                .create(getLogger())
                .db(context.getProperty(TARGET_CONNECTION_POOL_PROPERTY).asControllerService(DBCPService.class))
                .matchStrategy(context.getProperty(MATCH_STRATEGY_PROPERTY).getValue())
                .targetTable(context.getProperty(TARGET_TABLE_NAME_PROPERTY).getValue())
                .keyColumns(context.getProperty(KEY_COLUMNS_PROPERTY).getValue());

        try (InputStream in = session.read(originalFF)) {
            try (RecordReader reader = srcReaderFactory.createRecordReader(originalFF, in, getLogger())) {
                upserter.upsert(reader);
            }

            session.transfer(originalFF, SUCCESS_RELATIONSHIP);

        } catch (Exception e) {
            getLogger().error("Failed to read records from {}; routing to failure", new Object[]{originalFF, e});
            session.transfer(originalFF, FAILURE_RELATIONSHIP);
        }
    }
}
