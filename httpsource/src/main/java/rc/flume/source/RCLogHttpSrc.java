package rc.flume.source;


import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadBase;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

public class RCLogHttpSrc implements HTTPSourceHandler {

    private static final String APP_KEY = "RC-App-Key";
    private static final String USER_ID = "RC-User-ID";
    private static final String SDK_VERSION = "RC-SDK-Version";
    private static final String PLATFORM = "RC-Platform";
    private static final String START_TIME = "RC-Start-Time";
    private static final String END_TIME = "RC-End-Time";
    private static final String USER_IP = "RC-User-IP";
    private static final String X_Real_IP = "X-Real-IP";

    private static final Logger logger = Logger.getLogger(RCLogHttpSrc.class);

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
        headerMap.put(USER_IP, userIp == null ? "" : userIp);
        headerMap.put(APP_KEY, appKey == null ? "" : appKey);
        headerMap.put(USER_ID, userId == null ? "" : userId);
        headerMap.put(SDK_VERSION, sdkVer == null ? "" : sdkVer);
        headerMap.put(PLATFORM, platform == null ? "" : platform);

        List<Event> eventList = new ArrayList<Event>();
        if (request.getMethod().equals("POST")) {
            ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());
            List<FileItem> items = upload.parseRequest(request);
            for (FileItem fileItem : items) {
                eventList.add(EventBuilder.withBody(fileItem.get(), headerMap));
            }
        }
        return eventList;
    }
}
