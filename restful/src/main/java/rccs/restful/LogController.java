package rccs.restful;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class LogController {

    private HbaseConnection conn = HbaseConnection.getInstance();

    @RequestMapping(value = "/log/{appKey}", method = RequestMethod.GET)
    public ResponseEntity<String> queryLogForApp(@PathVariable("appKey") String appKey) {
        String result = null;
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    /**
     * Accecpt HTTP GET request like "/log/{appKey}/{userId} for query log info,
     * or
     * Accecpt HTTP GET request like "/log/{appKey}/{userId}?start=xxxxxxxxxx&end=xxxxxxxxxx" for query log content.
     *
     * @param start 10-digit long type timestamp, must be same as the StartTime part of RowKey.
     * @param end   10-digit long type timestamp, must be same as the EndTime part of RowKey.
     * @return Json string list of log content.
     */
    @RequestMapping(value = "/log/{appKey}/{userId}", method = RequestMethod.GET)
    public ResponseEntity<String> queryLogForUser(@PathVariable("appKey") String appKey,
                                                  @PathVariable("userId") String userId,
                                                  String start, String end) {
        String result;
        try {
            if (start == null || end == null) {
                result = conn.getLogInfo(appKey, userId);
            } else {
                result = conn.getLogData(appKey, userId, start, end);
            }
        } catch (Exception e) {
            result = "Runtime exception!";
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}