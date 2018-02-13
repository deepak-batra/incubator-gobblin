package gobblin.fork;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Writable;

import java.rmi.server.UID;

public class CopyableAvroGenericRecordWritable implements Copyable<AvroGenericRecordWritable> {

    private final AvroGenericRecordWritable record;
    private final UID uid = new UID();

    public CopyableAvroGenericRecordWritable(AvroGenericRecordWritable record) {
        this.record = record;
    }

    @Override
    public AvroGenericRecordWritable copy() throws CopyNotSupportedException {

        // Make a deep copy of the original record
        AvroGenericRecordWritable avroWritable = new AvroGenericRecordWritable();
        avroWritable.setRecord(new GenericData.Record((GenericData.Record) record.getRecord(), true));
        avroWritable.setFileSchema(record.getFileSchema());
        avroWritable.setRecordReaderID(this.uid);
        return avroWritable;
    }
}
