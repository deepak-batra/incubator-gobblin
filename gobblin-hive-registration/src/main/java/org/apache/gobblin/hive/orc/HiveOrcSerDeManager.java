/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.hive.orc;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.HiveSerDeWrapper;
import org.apache.gobblin.hive.avro.HiveAvroSerDeManager;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.OrcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;

import java.io.IOException;

/**
 * A {@link HiveSerDeManager} for registering ORC tables and partitions.
 *
 * @author deepak.batra
 */
@Slf4j
@Alpha
public class HiveOrcSerDeManager extends HiveAvroSerDeManager {

    protected final HiveSerDeWrapper serDeWrapper;
    private final MetricContext metricContext ;

    public HiveOrcSerDeManager(State props) throws IOException {
        super(props);
        serDeWrapper = HiveSerDeWrapper.get("ORC");
        this.metricContext = Instrumented.getMetricContext(props, org.apache.gobblin.hive.orc.HiveOrcSerDeManager.class);
    }

    @Override
    public void addSerDeProperties(HiveRegistrationUnit source, HiveRegistrationUnit target) throws IOException {
        if (source.getSerDeType().isPresent()) {
            target.setSerDeType(source.getSerDeType().get());
        }
        if (source.getInputFormat().isPresent()) {
            target.setInputFormat(source.getInputFormat().get());
        }
        if (source.getOutputFormat().isPresent()) {
            target.setOutputFormat(source.getOutputFormat().get());
        }
        if (source.getSerDeProps().contains(SCHEMA_LITERAL)) {
            target.setSerDeProp(SCHEMA_LITERAL, source.getSerDeProps().getProp(SCHEMA_LITERAL));
        }
        if (source.getSerDeProps().contains(SCHEMA_URL)) {
            target.setSerDeProp(SCHEMA_URL, source.getSerDeProps().getProp(SCHEMA_URL));
        }
    }

    /**
     * Add an Orc {@link Schema} to the given {@link HiveRegistrationUnit}.
     *
     *  <p>
     *    If {@link #USE_SCHEMA_FILE} is true, the schema will be added via {@link #SCHEMA_URL} pointing to
     *    the schema file named {@link #SCHEMA_FILE_NAME}.
     *  </p>
     *
     *  <p>
     *    If {@link #USE_SCHEMA_FILE} is false, the schema will be obtained by {@link #getDirectorySchema(Path)}.
     *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via
     *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
     *    via {@link #SCHEMA_URL}.
     *  </p>
     */
    @Override
    public void addSerDeProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
        hiveUnit.setSerDeType(this.serDeWrapper.getSerDe().getClass().getName());
        hiveUnit.setInputFormat(this.serDeWrapper.getInputFormatClassName());
        hiveUnit.setOutputFormat(this.serDeWrapper.getOutputFormatClassName());

        addSchemaProperties(path, hiveUnit);
    }

    private void addSchemaProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
        Preconditions.checkArgument(this.fs.getFileStatus(path).isDirectory(), path + " is not a directory.");

        Path schemaFile = new Path(path, this.schemaFileName);
        Optional<Pair<String, String>> schema;
        if (this.useSchemaFile) {
            schema = Optional.of(OrcUtils.parseSchemaFromFile(schemaFile, fs));
            hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
        } else {
            try (Timer.Context context = metricContext.timer(HIVE_SPEC_SCHEMA_READING_TIMER).time()) {
                schema = getOrcDirectorySchema(path);
            }
        }
        try (Timer.Context context = metricContext.timer(HIVE_SPEC_SCHEMA_WRITING_TIMER).time()) {
            addSchemaFromOrcFile(schema, schemaFile, hiveUnit);
        }
    }

    /**
     * Get schema for a directory using {@link OrcUtils#getDirectorySchema(Path, FileSystem, boolean)}.
     */
    protected Optional<Pair<String,String>> getOrcDirectorySchema(Path directory) throws IOException {
        return OrcUtils.getDirectorySchema(directory, this.fs, true);
    }

    /**
     * Add a {@link Schema} obtained from an Orc data file to the given {@link HiveRegistrationUnit}.
     *
     *  <p>
     *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via
     *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
     *    via {@link #SCHEMA_URL}.
     *  </p>
     */
    protected void addSchemaFromOrcFile(Optional<Pair<String,String>> schema, Path schemaFile,
                                        HiveRegistrationUnit hiveUnit) throws IOException {
        Preconditions.checkArgument(schema.isPresent());
        hiveUnit.setSerDeProp(IOConstants.COLUMNS, schema.get().getLeft());
        hiveUnit.setSerDeProp(IOConstants.COLUMNS_TYPES, schema.get().getRight());
        if (schema.get().toString().length() <= this.schemaLiteralLengthLimit)
            hiveUnit.setSerDeProp(SCHEMA_LITERAL, schema.get().toString());
        else {
            OrcUtils.writeSchemaToFile(schema.get(), schemaFile, this.fs, true);
            log.info("Using schema file " + schemaFile.toString());
            hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
        }
    }

}