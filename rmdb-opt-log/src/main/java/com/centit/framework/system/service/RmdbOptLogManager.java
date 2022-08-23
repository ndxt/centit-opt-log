package com.centit.framework.system.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.dao.RmdbOptLogDao;
import com.centit.framework.system.po.RmdbOptLog;
import com.centit.support.database.utils.PageDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("rmdbOptLogManager")
public class RmdbOptLogManager implements OperationLogManager {

    private RmdbOptLogDao optLogDao;

    @Autowired
    @NotNull
    public void setOptLogDao(RmdbOptLogDao optLogDao) {
        this.optLogDao = optLogDao;
    }


    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveOptLog(OperationLog optLog){
        RmdbOptLog rmdbOptLog = RmdbOptLog.valueOf(optLog);
        optLogDao.saveNewObject(rmdbOptLog);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveBatchOptLogs(List<OperationLog> optLogs) {
        if (CollectionUtils.isEmpty(optLogs)) {
            return;
        }
        for (OperationLog optLog : optLogs) {
            RmdbOptLog rmdbOptLog = RmdbOptLog.valueOf(optLog);
            optLogDao.saveNewObject(rmdbOptLog);
        }

    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public int delete(String begin) {
        return optLogDao.delete(begin);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void deleteMany(String[] logIds) {
        for (String logId : logIds) {
            optLogDao.deleteObjectById(logId);
        }
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public JSONArray listOptLogsAsJson(String[] fields,
                                       Map<String, Object> filterMap, PageDesc pageDesc){
        return DictionaryMapUtils.mapJsonArray(
                    optLogDao.listObjectsPartFieldAsJson( filterMap, fields ,pageDesc),
                    RmdbOptLog.class);
    }

    @Override
    @Transactional
    public OperationLog getOptLogById(String logId) {
        RmdbOptLog rmdbOptLog = optLogDao.getObjectById(logId);
        return rmdbOptLog.toOperationLog();
    }


    @Override
    @Transactional
    public void deleteOptLogById(String logId) {
        optLogDao.deleteObjectById(logId);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void save(final OperationLog optLog) {
        RmdbOptLog optlog = RmdbOptLog.valueOf(optLog);
        optLogDao.saveNewObject(optlog);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void save(List<OperationLog> optLogs) {
        for(OperationLog optlog : optLogs){
            optLogDao.saveNewObject(RmdbOptLog.valueOf(optlog));
        }
    }

    @Override
    public List<? extends OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows) {
        filterMap.put("optId", optId);
        List<RmdbOptLog> optlogs = (startPos >= 0 && maxRows > 0) ?
            optLogDao.listObjects(filterMap, new PageDesc(startPos, maxRows)):
            optLogDao.listObjects(filterMap);
        if(optlogs==null || optlogs.size()==0)
            return null;
        return optlogs.stream().map(RmdbOptLog::toOperationLog).collect(Collectors.toList());
    }

    @Override
    public int countOptLog(String optId, Map<String, Object> filterMap) {
        filterMap.put("optId", optId);
        return optLogDao.countObject(filterMap);
    }

}
