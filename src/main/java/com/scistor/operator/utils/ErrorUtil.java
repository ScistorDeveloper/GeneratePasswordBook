package com.scistor.operator.utils;

import org.apache.commons.logging.Log;

/**
 * Created by Administrator on 2017/11/8.
 */
public class ErrorUtil {

	public static void ErrorLog(Log LOG, Exception e) {
		StackTraceElement[] stackTraceElements = e.getStackTrace();
		for (StackTraceElement stackTraceElement : stackTraceElements) {
			LOG.error(stackTraceElement);
		}
	}

}
