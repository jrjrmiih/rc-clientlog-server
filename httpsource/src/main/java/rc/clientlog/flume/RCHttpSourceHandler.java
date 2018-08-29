package rc.clientlog.flume;


import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

public class RCHttpSourceHandler implements HTTPSourceHandler {

    private static final String NULL = "NULL";

    private static final String HEADER_APP_KEY = "RC-App-Key";
    private static final String HEADER_USER_ID = "RC-User-ID";
    private static final String HEADER_SDK_VERSION = "RC-SDK-Version";
    private static final String HEADER_PLATFORM = "RC-Platform";
    private static final String HEADER_START_TIME = "RC-Start-Time";
    private static final String HEADER_END_TIME = "RC-End-Time";
    private static final String HEADER_USER_IP = "RC-User-IP";
    private static final String X_Real_IP = "X-Real-IP";

    private static Logger logger = Logger.getLogger(RCHttpSourceHandler.class);

    public void configure(Context context) {
    }

    public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
        Map<String, String> headerMap = new HashMap<String, String>();
        String appKey = request.getHeader(HEADER_APP_KEY);
        String userId = request.getHeader(HEADER_USER_ID);
        String sdkVer = request.getHeader(HEADER_SDK_VERSION);
        String platform = request.getHeader(HEADER_PLATFORM);

        // When using nginx or proxy, getRemoteAddr() may return null.
        // In this scenario, the real user IP will be set in 'X_Real_IP',
        // or the first section split by ',' of 'X-Forwarded-For'.
        String xRealIp = request.getHeader(X_Real_IP);
        String remoteAddr = request.getRemoteAddr();
        String userIp = TextUtils.isEmpty(xRealIp) ? (TextUtils.isEmpty(remoteAddr) ? NULL : remoteAddr) : xRealIp;
        headerMap.put(HEADER_USER_IP, RCUtils.getValidStr(userIp, NULL));
        headerMap.put(HEADER_APP_KEY, RCUtils.getValidStr(appKey, NULL));
        headerMap.put(HEADER_USER_ID, RCUtils.getValidStr(userId, NULL));
        headerMap.put(HEADER_SDK_VERSION, RCUtils.getValidStr(sdkVer, NULL));
        headerMap.put(HEADER_PLATFORM, RCUtils.getValidStr(platform, NULL));

        List<Event> eventList = new ArrayList<Event>();
        ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
        List<FileItem> items = upload.parseRequest(request);
        for (FileItem fileItem : items) {
            String[] timeRange = RCUtils.getTimeRangeInGz(fileItem.get());
            headerMap.put(HEADER_START_TIME, RCUtils.getValidStr(timeRange[0], NULL));
            headerMap.put(HEADER_END_TIME, RCUtils.getValidStr(timeRange[1], NULL));
            checkValidity(headerMap);
            eventList.add(EventBuilder.withBody(fileItem.get(), headerMap));
        }
        return eventList;
    }

    private void checkValidity(Map<String, String> headerMap) {
        if (headerMap.get(HEADER_USER_ID).equals(NULL)) {
            String platform = headerMap.get(HEADER_PLATFORM);
            String sdkVer = headerMap.get(HEADER_SDK_VERSION);
            logger.error("User ID not valid: " + platform + "_" + sdkVer);
        }
    }
}
