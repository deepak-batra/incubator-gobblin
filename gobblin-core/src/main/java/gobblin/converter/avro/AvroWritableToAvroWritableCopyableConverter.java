package gobblin.converter.avro;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.fork.CopyableAvroGenericRecordWritable;
import gobblin.fork.CopyableSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

public class AvroWritableToAvroWritableCopyableConverter extends
        Converter<Schema, CopyableSchema, AvroGenericRecordWritable, CopyableAvroGenericRecordWritable> {

    /**
     * Returns a {@link gobblin.fork.CopyableSchema} wrapper around the given {@link Schema}.
     * {@inheritDoc}
     * @see gobblin.converter.Converter#convertSchema(java.lang.Object, gobblin.configuration.WorkUnitState)
     */
    @Override
    public CopyableSchema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
        return new CopyableSchema(inputSchema);
    }

    /**
     * Returns a {@link gobblin.fork.CopyableGenericRecord} wrapper around the given {@link GenericRecord}.
     * {@inheritDoc}
     * @see gobblin.converter.Converter#convertRecord(java.lang.Object, java.lang.Object, gobblin.configuration.WorkUnitState)
     */
    @Override
    public Iterable<CopyableAvroGenericRecordWritable> convertRecord(CopyableSchema outputSchema, AvroGenericRecordWritable inputRecord,
                                                         WorkUnitState workUnit) throws DataConversionException {
        return new SingleRecordIterable<>(new CopyableAvroGenericRecordWritable(inputRecord));
    }
}

