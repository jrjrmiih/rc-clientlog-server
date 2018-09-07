package rc.flume.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RCLogHttpInter implements Interceptor {

    private static final String APP_KEY = "RC-App-Key";
    private static final String USER_ID = "RC-User-ID";
    private static final String SDK_VERSION = "RC-SDK-Version";
    private static final String PLATFORM = "RC-Platform";
    private static final String START_TIME = "RC-Start-Time";
    private static final String END_TIME = "RC-End-Time";
    private static final String USER_IP = "RC-User-IP";

    private static final String MINI_SDK_VER = "2.8.10";

    private static final Logger logger = Logger.getLogger(RCLogHttpInter.class);

    private RCHttpInterCounter httpInterCount;

    public void initialize() {
        httpInterCount = new RCHttpInterCounter("RCLogInterceptor");
        httpInterCount.start();
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String sdkVer = headers.get(SDK_VERSION);
        if (sdkVer == null) {
            httpInterCount.incrementSdkVerNullCount();
        } else if (RCUtils.compareVer(sdkVer, MINI_SDK_VER) < 0) {
            httpInterCount.incrementSdkVerOlderCount();
        } else {
            try {
                String[] timeRange = RCUtils.getTimeRangeInGz(event.getBody());
                headers.put(START_TIME, timeRange[0] == null ? "" : timeRange[0]);
                headers.put(END_TIME, timeRange[1] == null ? "" : timeRange[1]);
                event.setHeaders(headers);
                return event;
            } catch (IOException e) {
                httpInterCount.incrementUnzipFailedCount();
            }
        }
        return null;
    }

    public List<Event> intercept(List<Event> events) {
        List<Event> doneEvents = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event doneEvent = intercept(event);
            if (doneEvent != null) {
                doneEvents.add(doneEvent);
            }
        }
        return doneEvents;
    }

    public void close() {
        httpInterCount.stop();
    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new RCLogHttpInter();
        }

        public void configure(Context context) {
        }
    }
}
