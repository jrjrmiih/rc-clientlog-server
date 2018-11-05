package rccs.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RccsHbaseSinkSer implements HbaseEventSerializer {

    private static final String HEADER_APP_KEY = "aid";
    private static final String HEADER_USER_ID = "uid";
    private static final String HEADER_SDK_VER = "ver";
    private static final String HEADER_PLATFORM = "os";
    private static final String HEADER_USER_IP = "uip";
    private static final String HEADER_START_TIME = "st";
    private static final String HEADER_END_TIME = "et";

    private static final byte[] QF_GZ = "gz".getBytes();
    private static final byte[] QF_OS = "os".getBytes();
    private static final byte[] QF_VER = "ver".getBytes();
    private static final byte[] QF_IP = "ip".getBytes();

    private Event event;
    private byte[] cf;

    public void initialize(Event event, byte[] cf) {
        this.event = event;
        this.cf = cf;
    }

    public List<Row> getActions() {
        List<Row> actions = new LinkedList<Row>();
        Map<String, String> header = event.getHeaders();
        Put put = new Put(getDataRowKey(header));
        put.addColumn(cf, QF_GZ, event.getBody());
        put.addColumn(cf, QF_OS, header.get(HEADER_PLATFORM).getBytes());
        put.addColumn(cf, QF_VER, header.get(HEADER_SDK_VER).getBytes());
        put.addColumn(cf, QF_IP, header.get(HEADER_USER_IP).getBytes());
        actions.add(put);
        return actions;
    }

    public List<Increment> getIncrements() {
        return new ArrayList<Increment>();
    }

    public void close() {

    }

    public void configure(Context context) {

    }

    public void configure(ComponentConfiguration componentConfiguration) {

    }

    private byte[] getDataRowKey(@NotNull Map<String, String> header) {
        String appKey = header.get(HEADER_APP_KEY);
        String userId = header.get(HEADER_USER_ID);
        String startTime = header.get(HEADER_START_TIME);
        String endTime = header.get(HEADER_END_TIME);
        String salt = getSaltStr(userId);
        return (salt + "^" + appKey + "^" + userId + "^" + startTime + "^" + endTime).getBytes();
    }

    private String getSaltStr(@NotNull String userId) {
        return String.format("%02d", Math.abs(userId.hashCode() % 100));
    }
}
