package com.centit.framework.system.dao;

import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.framework.system.po.RmdbOptLog;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Repository("optLogDao")
public class RmdbOptLogDao extends BaseDaoImpl<RmdbOptLog, String> {

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("logId", CodeBook.EQUAL_HQL_ID);
        filterField.put("logLevel", CodeBook.LIKE_HQL_ID);
        filterField.put(CodeRepositoryUtil.USER_CODE, CodeBook.EQUAL_HQL_ID);
        filterField.put("(date)optTimeBegin", "optTime >= :optTimeBegin ");
        filterField.put("(nextday)optTimeEnd", "optTime < :optTimeEnd");
        filterField.put("optId", CodeBook.LIKE_HQL_ID);
        filterField.put("optCode", CodeBook.LIKE_HQL_ID);
        filterField.put("optContent", CodeBook.LIKE_HQL_ID);
        filterField.put("oldValue", CodeBook.LIKE_HQL_ID);
        filterField.put("optMethod", CodeBook.EQUAL_HQL_ID);
        filterField.put("optTag", CodeBook.EQUAL_HQL_ID);
        filterField.put("userCode", CodeBook.EQUAL_HQL_ID);
        filterField.put("unitCode_in", "UNIT_CODE in (:unitCode_in)");
        return filterField;
    }

    public RmdbOptLog getObjectById(String logId) {
        return super.getObjectById(logId);
    }

    @Transactional
    public void deleteObjectById(String logId) {
        super.deleteObjectById(logId);
    }

    @Transactional
    public int delete(String begin) {
        String delSql = "delete from F_OPT_LOG  where LOG_LEVEL = '0' and  OPT_TIME <= ? ";
        return DatabaseOptUtils.doExecuteSql(this, delSql,new Object[]{begin});
    }

}
