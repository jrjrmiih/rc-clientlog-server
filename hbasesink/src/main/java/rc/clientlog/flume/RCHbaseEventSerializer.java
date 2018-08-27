package rc.clientlog.flume;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RCHbaseEventSerializer implements HbaseEventSerializer {

    private static final byte[] QF_GZ = "gz".getBytes(Charsets.UTF_8);
    private static final byte[] QF_VER = "ver".getBytes(Charsets.UTF_8);
    private static final byte[] QF_IP = "ip".getBytes(Charsets.UTF_8);

    private Event event;
    private byte[] cf;

    public void initialize(Event event, byte[] cf) {
        this.event = event;
        this.cf = cf;
    }

    public List<Row> getActions() {
        List<Row> actions = new LinkedList<Row>();
        Map<String, String> header = event.getHeaders();
        Put put = new Put(getRowKey(header));
        put.addColumn(cf, QF_GZ, event.getBody());
        put.addColumn(cf, QF_VER, header.get(RCHeaderName.SDK_VERSION).getBytes(Charsets.UTF_8));
        put.addColumn(cf, QF_IP, header.get(RCHeaderName.USER_IP).getBytes(Charsets.UTF_8));
        actions.add(put);
        return actions;
    }

    public List<Increment> getIncrements() {
        return new LinkedList<Increment>();
    }

    public void close() {

    }

    public void configure(Context context) {

    }

    public void configure(ComponentConfiguration componentConfiguration) {

    }

    private byte[] getRowKey(Map<String, String> header) {
        String appKey = header.get(RCHeaderName.APP_KEY);
        String userId = header.get(RCHeaderName.USER_ID).isEmpty() ? "null" : header.get(RCHeaderName.USER_ID);
        String startTime = getTimeStr(header.get(RCHeaderName.START_TIME));
        String endTime = getTimeStr(header.get(RCHeaderName.END_TIME));
        String salt = getSaltStr(appKey, userId);
        String now = String.valueOf(System.currentTimeMillis());

        String rowKey = salt + "_" + appKey + "_" + userId + "_" + startTime + "_" + endTime + "_" + now;
        return rowKey.getBytes(Charsets.UTF_8);
    }

    private String getTimeStr(String timestamp) {
        return timestamp.isEmpty() ? "null" : timestamp.substring(0, 10);
    }

    private String getSaltStr(String appKey, String userId) {
        if (userId.equals("null")) {
            String now = String.valueOf(System.currentTimeMillis());
            return MD5Hash.getMD5AsHex((appKey + userId + now).getBytes()).substring(0, 4);
        } else {
            return MD5Hash.getMD5AsHex((appKey + userId).getBytes()).substring(0, 4);
        }
    }
}
