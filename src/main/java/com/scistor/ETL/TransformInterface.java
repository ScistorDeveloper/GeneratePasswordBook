package com.scistor.ETL;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Administrator on 2017/11/10.
 */
public interface TransformInterface {

	void init(Map<String,String> config, ArrayBlockingQueue<Map> queue);
	List<String> validate();//参数校验
	void process() throws Exception;//编写业务处理逻辑
	void merge();//编写聚合逻辑
	void close();//关闭所用的资源

}
