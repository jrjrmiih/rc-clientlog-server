package rc.flume.interceptor;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

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

    public void initialize() {
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String appKey = headers.get(APP_KEY);
        String sdkVer = headers.get(SDK_VERSION);
        if (appKey == null) {
            System.out.println("appKey == null");
            return null;
        } else if (!RCUtils.isValidVer(sdkVer, "2.8.10")) {
            System.out.println("sdkVer == " + sdkVer);
            return null;
        } else {
            return event;
        }
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
    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new RCLogHttpInter();
        }

        public void configure(Context context) {
        }
    }
}
