package com.dataaggregation.utility;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * @author ShaishavS Utility Class to return error response
 */
@Component
public class ErrorUtil {

	/**
	 * This method will return the error response
	 * 
	 * @param exceptionMsg
	 * @param errorCode
	 * @return
	 */
	public String getDataAggregatorError(String exceptionMsg, String errorCode) {
		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("status", "error");
			jsonObject.put("code", errorCode);
			jsonObject.put("response", exceptionMsg);
		} catch (JSONException e) {
			return e.getMessage();
		}
		return jsonObject.toString();
	}
}
