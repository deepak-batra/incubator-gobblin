package gobblin.converter.serde;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;

public class OrcSerDeWrapper extends OrcSerde {
    private OrcSerde record = null;

    @Override
    public Writable serialize(Object realRow, ObjectInspector inspector) {
        record = new OrcSerde();
        Object realRowClone = ((ArrayList) realRow).clone();
        return record.serialize(realRowClone, inspector);
    }
}
