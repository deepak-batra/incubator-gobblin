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

package gobblin.converter.serde;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.extract.kafka.ConfluentKafkaSchemaRegistry;
import gobblin.source.extractor.extract.kafka.SimpleKafkaSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.hive.HiveSerDeWrapper;
import gobblin.instrumented.converter.InstrumentedConverter;
import gobblin.util.HadoopUtils;


/**
 * An {@link InstrumentedConverter} that takes a {@link Writable} record, uses a Hive {@link SerDe} to
 * deserialize it, and uses another Hive {@link SerDe} to serialize it into a {@link Writable} record.
 *
 * The serializer and deserializer are specified using {@link HiveSerDeWrapper#SERDE_SERIALIZER_TYPE}
 * and {@link HiveSerDeWrapper#SERDE_DESERIALIZER_TYPE}.
 *
 * <p>
 *   Note this class has known issues when the {@link #serializer} is set to
 *   {@link org.apache.hadoop.hive.serde2.avro.AvroSerializer}. Mainly due to the fact that the Avro Serializer caches
 *   returned objects, which are not immediately consumed by the
 *   {@link org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat}.
 * </p>
 *
 * <p>
 *   This class has been tested when the {@link #serializer} has been set to
 *   {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde} and should work as expected assuming the proper configurations
 *   are set (refer to the Gobblin documentation for a full example).
 * </p>
 *
 * @author Ziyang Liu
 */
@SuppressWarnings("deprecation")
@Slf4j
public class HiveSerDeConverter extends InstrumentedConverter<Object, Object, Writable, Writable> {

  private SerDe serializer;
  private SerDe deserializer;
  private String topicName;
  private KafkaSchemaRegistry<?, ?> kafkaConfluentSchemaRegistry;
  private Schema latestSchema;
  private static final int MAX_NUM_OF_RETRY = 3;
  public static final String SCHEMA_LITERAL = "avro.schema.literal";

  @Override
  public HiveSerDeConverter init(WorkUnitState state) {
    super.init(state);
    Configuration conf = HadoopUtils.getConfFromState(state);

    try {
      this.serializer = HiveSerDeWrapper.getSerializer(state).getSerDe();
      this.deserializer = HiveSerDeWrapper.getDeserializer(state).getSerDe();

      // Change Starts
      Properties properties = state.getProperties();
      this.topicName = properties.getProperty("topic.name");
      this.kafkaConfluentSchemaRegistry = getKafkaConfluentSchemaRegistry(properties);
      this.latestSchema = (Schema) getSchema();
      // forcefully overwriting the SCHEMA_LITERAL
      state.setProp(SCHEMA_LITERAL, String.valueOf(latestSchema));
      // Change Ends

      this.deserializer.initialize(conf, state.getProperties());
      setColumnsIfPossible(state);
      this.serializer.initialize(conf, state.getProperties());
    } catch (IOException e) {
      log.error("Failed to instantiate serializer and deserializer", e);
      throw Throwables.propagate(e);
    } catch (SerDeException e) {
      log.error("Failed to initialize serializer and deserializer", e);
      throw Throwables.propagate(e);
    }

    return this;
  }

  private void setColumnsIfPossible(WorkUnitState state)
      throws SerDeException {
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(
        AvroSerdeUtils.determineSchemaOrReturnErrorSchema(state.getProperties()));
    List<String> columnNames = aoig.getColumnNames();
    List<TypeInfo> columnTypes = aoig.getColumnTypes();

    state.setProp(IOConstants.COLUMNS, StringUtils.join(columnNames, ","));
    state.setProp(IOConstants.COLUMNS_TYPES, StringUtils.join(columnTypes, ","));
  }

  @Override
  public Iterable<Writable> convertRecordImpl(Object outputSchema, Writable inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    for (int i = 0; i < this.MAX_NUM_OF_RETRY; i++) {
      try {
        Object deserialized = this.deserializer.deserialize(inputRecord);
        Writable convertedRecord = this.serializer.serialize(deserialized, this.deserializer.getObjectInspector());
        return new SingleRecordIterable<>(convertedRecord);
      } catch (SerDeException e) {
        throw new DataConversionException(e);
      } catch (IllegalArgumentException e) {
        log.warn(String.format("Deserialization has failed %d time(s). Reason: %s", i + 1,
            e));
        if (i < this.MAX_NUM_OF_RETRY - 1) {
          try {
            Thread.sleep((long) ((i + Math.random()) * 1000));
          } catch (InterruptedException e2) {
            log.error("Caught interrupted exception between retries of deserailization. " + e2);
          }
        }
        throw new IllegalArgumentException(e);
      } catch (BufferUnderflowException e) {
        log.warn(String.format("Deserialization has failed %d time(s). Reason: %s", i + 1,
            e));
        if (i < this.MAX_NUM_OF_RETRY - 1) {
          try {
            Thread.sleep((long) ((i + Math.random()) * 1000));
          } catch (InterruptedException e2) {
            log.error("Caught interrupted exception between retries of deserailization. " + e2);
          }
        }
        throw new BufferUnderflowException();
      }
    }
    throw new DataConversionException(String.format("Deserialization failed for inputRecord"));
  }

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  // Change Starts
  private static KafkaSchemaRegistry<?, ?> getKafkaConfluentSchemaRegistry(Properties props) {
    try {
      return ConstructorUtils.invokeConstructor(ConfluentKafkaSchemaRegistry.class, props);
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return new SimpleKafkaSchemaRegistry(props);
  }

  public Object getSchema() {
    try {
      return this.kafkaConfluentSchemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }
  // Change Ends
}
