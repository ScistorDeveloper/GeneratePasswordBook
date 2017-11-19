package com.scistor.process.record.extend;

import com.scistor.process.record.Record;

import java.util.Map;

/**
 * Created by WANG Shenghua on 2017/11/17.
 */

//HTTP（POST/GET类型数据）每一条抽象为一个MAP格式
public class HttpRecord extends Record {

    private Map<String, String> record;

    public HttpRecord(Map<String, String> record) {
        this.record = record;
    }

    public Map<String, String> getRecord() {
        return record;
    }

    public void setRecord(Map<String, String> record) {
        this.record = record;
    }
}
