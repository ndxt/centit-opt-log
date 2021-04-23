package com.centit.framework.system.dao;

import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.framework.system.po.OptLog;
import com.centit.support.database.utils.PersistenceException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Repository("optLogDao")
public class OptLogDao extends BaseDaoImpl<OptLog, String> {

    //public static final Logger logger = LoggerFactory.getLogger(OptLogDao.class);

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

    public OptLog getObjectById(String logId) {
        return super.getObjectById(logId);
    }

    @Transactional
    public void deleteObjectById(String logId) {
        super.deleteObjectById(logId);
    }

    @Transactional
    public void delete(Date begin, Date end) {
        String hql = "delete from F_OPT_LOG o where 1=1 ";
        List<Object> objects = new ArrayList<>();
        if (null != begin) {
            hql += "and o.optTime > ?";
            objects.add(begin);
        }
        if (null != end) {
            hql += "and o.optTime < ?";
            objects.add(end);
        }

        try {
            DatabaseOptUtils.doExecuteSql(this, hql, objects.toArray(new Object[objects.size()]));
        } catch (DataAccessException e) {
            throw new PersistenceException(e);
        }

    }

}
