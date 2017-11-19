package com.scistor.process.pojo;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by Administrator on 2017/11/7.
 */
public class SlavesLocation {

	private String ip;
	private int port;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return "SlavesLocation{" +
				"ip='" + ip + '\'' +
				", port=" + port +
				'}';
	}

	public String toJsonString(){
		JSONObject obj=new JSONObject();
		obj.put("ip", ip);
		obj.put("port", port);
		return obj.toString();
	}

}
