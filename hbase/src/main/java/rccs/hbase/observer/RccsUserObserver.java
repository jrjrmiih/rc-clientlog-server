package rccs.hbase.observer;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;

public class RccsUserObserver extends BaseRegionObserver {

    private static final String T_USER_RECORD = "user_record";

    private static final Logger logger = Logger.getLogger(RccsLogStateObserver.class);
    private static Table tLogState;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        Connection conn = ConnectionFactory.createConnection();
        tLogState = conn.getTable(TableName.valueOf(T_USER_RECORD));
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        String[] split = new String(put.getRow()).split("\\^");
        String appKey = split[1];
        String userId = split[2];
        String dayStart = RccsLogStateObserver.timestamp2Date(split[3]);
        String dayEnd = RccsLogStateObserver.timestamp2Date(split[4]);

        if (isValidTime(split[3])) {
            Put record = new Put((appKey + "_" + dayStart).getBytes());
            record.addColumn("uid".getBytes(), userId.getBytes(), "".getBytes());
            tLogState.put(record);
        }
        if (!dayEnd.equals(dayStart) && isValidTime(split[4])) {
            Put record = new Put((appKey + "_" + dayEnd).getBytes());
            record.addColumn("uid".getBytes(), userId.getBytes(), "".getBytes());
            tLogState.put(record);
        }
    }

    private static boolean isValidTime(String timestamp) {
        Date date = new Date(Long.parseLong(timestamp + "000"));
        Date now = new Date();
        long delta = now.getTime() - date.getTime();
        return (delta > -24 * 60 * 60 * 1000) && (delta < 7 * 24 * 60 * 60 * 1000);
    }
}