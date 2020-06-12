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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"etl", "sql"})
@CapabilityDescription("Loads FlowFile data to a DB table. Rows missing in DB are inserted, rows already in DB are updated.")
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class UpsertSQL extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_POOL_PROPERTY = new PropertyDescriptor.Builder()
            .name("connection-pool")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME_PROPERTY = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("target table name")
            .description("The name of the target table for to insert or update data")
            .required(true)
            .allowableValues(MatchStrategy.values())
            .defaultValue(MatchStrategy.pk.name())
            .build();

    static final PropertyDescriptor MATCH_STRATEGY_PROPERTY = new PropertyDescriptor.Builder()
            .name("match-strategy")
            .displayName("row match strategy")
            .description("How should we identify existing rows to be updated")
            .required(false)
            .allowableValues(MatchStrategy.values())
            .defaultValue(MatchStrategy.pk.name())
            .build();

    static final PropertyDescriptor KEY_COLUMNS_PROPERTY = new PropertyDescriptor.Builder()
            .name("key-columns")
            .displayName("key columns")
            .description("A comma-separated list of the column names in the target table to use for row matching")
            .required(false)
            .build();

    static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("TODO: a FlowFile with update stats")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        this.descriptors = Collections.unmodifiableList(asList(
                CONNECTION_POOL_PROPERTY,
                TABLE_NAME_PROPERTY,
                MATCH_STRATEGY_PROPERTY,
                KEY_COLUMNS_PROPERTY));

        this.relationships = Collections.unmodifiableSet(new HashSet<>(asList(
                SUCCESS_RELATIONSHIP)));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // TODO implement
    }
}
