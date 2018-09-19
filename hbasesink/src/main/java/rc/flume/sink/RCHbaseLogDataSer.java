package rc.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RCHbaseLogDataSer implements HbaseEventSerializer {

    private static final String HEADER_APP_KEY = "AppKey";
    private static final String HEADER_USER_ID = "UserId";
    private static final String HEADER_SDK_VER = "SdkVer";
    private static final String HEADER_PLATFORM = "Platform";
    private static final String HEADER_USER_IP = "UserIp";
    private static final String HEADER_START_TIME = "Start";
    private static final String HEADER_END_TIME = "End";

    private static final byte[] QF_GZ = "gz".getBytes();
    private static final byte[] QF_OS = "os".getBytes();
    private static final byte[] QF_VER = "ver".getBytes();
    private static final byte[] QF_IP = "ip".getBytes();

    private static final int SALT_LEN = 2;
    private static final String TABLE_SUM = "logsum";
//    private static final Logger logger = Logger.getLogger(RCHbaseLogDataSer.class);

    private Table logSum;
    private Event event;
    private byte[] cf;

    public RCHbaseLogDataSer() {
        try {
            Configuration config = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(config);
            logSum = conn.getTable(TableName.valueOf(TABLE_SUM));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        Map<String, String> header = event.getHeaders();
        Increment inc = new Increment(getSumRowKey(header));
        String platform = header.get(HEADER_PLATFORM);
        String sdkVer = header.get(HEADER_SDK_VER);
        inc.addColumn("sum".getBytes(), (platform + "_" + sdkVer).getBytes(), 1L);
        try {
            logSum.increment(inc);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new LinkedList<Increment>();
    }

    public void close() {

    }

    public void configure(Context context) {

    }

    public void configure(ComponentConfiguration componentConfiguration) {

    }

    private byte[] getDataRowKey(Map<String, String> header) {
        String appKey = header.get(HEADER_APP_KEY);
        String userId = header.get(HEADER_USER_ID);
        String startTime = header.get(HEADER_START_TIME);
        String endTime = header.get(HEADER_END_TIME);
        String salt = getSaltStr(appKey, userId);
        return (salt + "^" + appKey + "^" + userId + "^" + startTime + "^" + endTime).getBytes();
    }

    private String getSaltStr(String appKey, String userId) {
        if (userId.equals("")) {
            String now = String.valueOf(System.currentTimeMillis());
            return MD5Hash.getMD5AsHex((appKey + now).getBytes()).substring(0, SALT_LEN);
        } else {
            return MD5Hash.getMD5AsHex((appKey + userId).getBytes()).substring(0, SALT_LEN);
        }
    }

    private byte[] getSumRowKey(Map<String, String> header) {
        String appKey = header.get(HEADER_APP_KEY);
        String startTime = header.get(HEADER_START_TIME);
        String date = timestamp2Date(startTime);
        return (appKey + "_" + date).getBytes();
    }

    private static String timestamp2Date(String timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        return sdf.format(new Date(Long.valueOf(timestamp + "000")));
    }
}
