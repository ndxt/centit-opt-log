package com.centit.framework.system.service;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.support.database.utils.PageDesc;

import java.util.Map;

public interface OperationLogManager extends OperationLogWriter {

    OperationLog getOptLogById(String logId);

    JSONArray listOptLogsAsJson(String[] fields, Map<String, Object> filterMap, PageDesc pageDesc);

    void deleteOptLogById(String logId);

    /**
     * 清理此日期之前的日志信息
     * @param beginDate 起始时间
     * @return 数据条数
     */
    int delete(String beginDate);

    void deleteMany(String[] logIds);
}

