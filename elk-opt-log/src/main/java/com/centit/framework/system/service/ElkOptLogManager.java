package com.centit.framework.system.service;

import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;

import java.util.List;

public class ElkOptLogManager implements OperationLogWriter {

    @Override
    public void save(OperationLog optLog) {

    }

    @Override
    public void save(List<OperationLog> optLogs) {
        for(OperationLog optLog : optLogs){
            save(optLog);
        }
    }
}
