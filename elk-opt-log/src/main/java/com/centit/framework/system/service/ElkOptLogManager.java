package com.centit.framework.system.service;

import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.po.ESOperationLog;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.Impl.ESIndexer;
import com.centit.support.common.ObjectException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service("elkOptLogManager")
public class ElkOptLogManager implements OperationLogWriter {

    @Autowired(required = false)
    private ESIndexer esObjectIndexer;

    @Override
    public void save(OperationLog operationLog) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,null);
        if (esObjectIndexer.saveNewDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    @Override
    public void save(List<OperationLog> optLogs) {
        for(OperationLog operationLog : optLogs){
            save(operationLog);
        }
    }

    public void deleteObjectById(String docId) {
        if (!esObjectIndexer.deleteDocument(docId)) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    public void updateOperationLog(OperationLog operationLog,String logId) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,logId);
        if (esObjectIndexer.mergeDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    /*
     * 日志精确查询
     * Map<String, Object> filterMap
     * 中为条件 sql ，key为字段，格式为 字段代码_表达式， 这个比较难实现
     * @see com.centit.support.database.jsonmaptable.buildFilterSql
     */
    @Override
    public List<? extends OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows){
        return null;
    }

    /*
     * 查询日志数量，这个不知道是否可以做到
     */
    @Override
    public int countOptLog(String optId, Map<String, Object> filter){
        return -1;
    }

}
