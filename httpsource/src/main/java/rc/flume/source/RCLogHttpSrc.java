package rc.flume.source;


import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RCLogHttpSrc implements HTTPSourceHandler {

    private static Logger logger = Logger.getLogger(RCLogHttpSrc.class);

    private static final String APP_KEY = "RC-App-Key";
    private static final String USER_ID = "RC-User-ID";
    private static final String SDK_VERSION = "RC-SDK-Version";
    private static final String PLATFORM = "RC-Platform";
    private static final String START_TIME = "RC-Start-Time";
    private static final String END_TIME = "RC-End-Time";
    private static final String USER_IP = "RC-User-IP";
    private static final String X_Real_IP = "X-Real-IP";
    
    public void configure(Context context) {
    }

    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        Map<String, String> headerMap = new HashMap<String, String>();
        String appKey = request.getHeader(APP_KEY);
        String userId = request.getHeader(USER_ID);
        String sdkVer = request.getHeader(SDK_VERSION);
        String platform = request.getHeader(PLATFORM);

        // When using nginx, getRemoteAddr() may return null.
        // In this scenario, the real user IP will be set in 'X_Real_IP',
        // or the first section split by ',' of 'X-Forwarded-For'.
        String xRealIp = request.getHeader(X_Real_IP);
        String remoteAddr = request.getRemoteAddr();
        String userIp = xRealIp == null ? remoteAddr : xRealIp;
        headerMap.put(USER_IP, userIp);
        headerMap.put(APP_KEY, appKey);
        headerMap.put(USER_ID, userId);
        headerMap.put(SDK_VERSION, sdkVer);
        headerMap.put(PLATFORM, platform);

        List<Event> eventList = new ArrayList<Event>();
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem fileItem : items) {
            String[] timeRange = RCUtils.getTimeRangeInGz(fileItem.get());
            headerMap.put(START_TIME, timeRange[0]);
            headerMap.put(END_TIME, timeRange[1]);
            checkValidity(headerMap);
            eventList.add(EventBuilder.withBody(fileItem.get(), headerMap));
        }
        return eventList;
    }

    private void checkValidity(Map<String, String> headerMap) {
        if (headerMap.get(USER_ID) == null) {
            String platform = headerMap.get(PLATFORM);
            String sdkVer = headerMap.get(SDK_VERSION);
            logger.error("User ID not valid: " + platform + "_" + sdkVer);
        }
    }

//    public static void main(String[] args) {
//    }
}
