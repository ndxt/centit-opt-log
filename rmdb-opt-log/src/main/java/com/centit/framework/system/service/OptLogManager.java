package com.centit.framework.system.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.system.po.OptLog;
import com.centit.support.database.utils.PageDesc;

import java.util.Date;
import java.util.List;
import java.util.Map;

public interface OptLogManager extends OperationLogWriter {

    OptLog getOptLogById(String logId);

    void deleteOptLogById(String logId);

    void saveOptLog(OptLog optLog);

    /**
     * 批量保存
     * @param optLogs List OptLog
     */
    void saveBatchOptLogs(List<OptLog> optLogs);
    /**
     * 清理此日期之间的日志信息
     *
     * @param begin Date
     */
    int delete(String begin);

    void deleteMany(String[] logIds);

    JSONArray listOptLogsAsJson(
        String[] fields,
        Map<String, Object> filterMap, PageDesc pageDesc);
}
