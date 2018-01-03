package gobblin.writer.partitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimeBasedTextWriterPartitioner extends TimeBasedWriterPartitioner<Text> {
    private static final Log LOG = LogFactory.getLog(TimeBasedTextWriterPartitioner.class);
    private static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    private int timestampIndex;
    private List<String> columnTokens;

    public TimeBasedTextWriterPartitioner(gobblin.configuration.State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        String columns = this.getColumns();
        this.columnTokens = new ArrayList<String>(Arrays.asList(columns.split(",")));
        this.timestampIndex = this.columnTokens.indexOf(TIMESTAMP_COLUMN_NAME);
    }

    @Override
    public long getRecordTimestamp(Text textRecord) {
        return getRecordTimestampUtil(textRecord, this.timestampIndex);
    }

    private static long getRecordTimestampUtil(Text textRecord, int timestampIndex) {
        long timestamp;
        String timestampAsString = "";
        String textRecordtokens[] = new String[0];
        try {
            String textRecordString = textRecord.toString();
            textRecordtokens = textRecordString.split("\u0001");
            timestampAsString = textRecordtokens[timestampIndex];
            timestamp = Long.valueOf(timestampAsString);
            LOG.debug("Timestamp of the Text Record: " + timestamp);
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Timestamp index: " + timestampIndex + ", but size of the array is: " + textRecordtokens.length);
            timestamp = System.currentTimeMillis();
        } catch (NumberFormatException e) {
            LOG.error("Timestamp Value: " + timestampAsString + ", text record val: " + textRecord.toString());
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
