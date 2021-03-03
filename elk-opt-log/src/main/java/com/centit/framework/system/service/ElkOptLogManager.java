package com.centit.framework.system.service;

import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.service.model.ESOperationLog;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.Impl.ESIndexer;
import com.centit.support.common.ObjectException;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Service("elkOptLogManager")
public class ElkOptLogManager implements OperationLogWriter {

    @Autowired(required = false)
    private ESIndexer esObjectIndexer;

    @Override
    public void save(OperationLog operationLog) {
        ESOperationLog esOperationLog = (ESOperationLog) operationLog;
        if (esObjectIndexer.saveNewDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    @Override
    public void save(List<OperationLog> optLogs) {
        for(OperationLog operationLog : optLogs){
            ESOperationLog esOperationLog = (ESOperationLog) operationLog;
            save(esOperationLog);
        }
    }

    public void deleteObjectById(String docId) {
        if (!esObjectIndexer.deleteDocument(docId)) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }


    public void updateOperationLog(OperationLog optLog) {
        if (esObjectIndexer.mergeDocument((ESOperationLog)optLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

}
