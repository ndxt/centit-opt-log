package com.centit.framework.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.dao.OptLogDao;
import com.centit.framework.system.po.OptLog;
import com.centit.framework.system.service.OptLogManager;
import com.centit.support.database.utils.PageDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("optLogManager")
public class OptLogManagerImpl implements OptLogManager {

    public static final Logger logger = LoggerFactory.getLogger(OptLogManager.class);


    private OptLogDao optLogDao;

    @Autowired
    @NotNull
    public void setOptLogDao(OptLogDao optLogDao) {
        this.optLogDao = optLogDao;
    }


    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveOptLog(OptLog optLog){
        optLogDao.saveNewObject(optLog);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void saveBatchOptLogs(List<OptLog> optLogs) {
        if (CollectionUtils.isEmpty(optLogs)) {
            return;
        }
        for (OptLog optLog : optLogs) {
            optLogDao.saveNewObject(optLog);
        }

    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void delete(Date begin, Date end) {
        optLogDao.delete(begin, end);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void deleteMany(Long[] logIds) {
        for (Long logId : logIds) {
            optLogDao.deleteObjectById(logId);
        }
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public JSONArray listOptLogsAsJson(String[] fields,
                                       Map<String, Object> filterMap, PageDesc pageDesc){
        //filterMap.put(CodeBook.TABLE_SORT_FIELD, "optTime");
        //filterMap.put("optId", new String[]{"login","admin","optTime"});
        return DictionaryMapUtils.mapJsonArray(
                    optLogDao.listObjectsPartFieldAsJson( filterMap, fields ,pageDesc),
                    OptLog.class);
    }

    @Override
    @Transactional
    public OptLog getOptLogById(Long logId) {
        return optLogDao.getObjectById(logId);
    }


    @Override
    @Transactional
    public void deleteOptLogById(Long logId) {
        optLogDao.deleteObjectById(logId);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void save(final OperationLog optLog) {
        OptLog optlog = OptLog.valueOf(optLog);
        optLogDao.saveNewObject(optlog);
    }

    @Override
    @Transactional(propagation=Propagation.REQUIRED)
    public void save(List<OperationLog> optLogs) {
        for(OperationLog optlog : optLogs){
            optLogDao.saveNewObject(OptLog.valueOf(optlog));
        }
    }

    @Override
    public List<? extends OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows) {
        filterMap.put("optId", optId);
        List<OptLog> optlogs = optLogDao.listObjectsByProperties(filterMap, startPos, maxRows);
        if(optlogs==null || optlogs.size()==0)
            return null;
        return optlogs.stream().map(OptLog::toOperationLog).collect(Collectors.toList());
    }

    @Override
    public int countOptLog(String optId, Map<String, Object> filterMap) {
        filterMap.put("optId", optId);
        return optLogDao.countObject(filterMap);
    }

}
