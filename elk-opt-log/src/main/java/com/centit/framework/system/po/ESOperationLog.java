
package com.centit.framework.system.po;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.search.annotation.ESField;
import com.centit.search.annotation.ESType;
import com.centit.search.document.ESDocument;
import com.centit.support.algorithm.DatetimeOpt;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.UUID;

@Data
@Entity
@ESType(indexName = "esoperationlog")
public class ESOperationLog  implements ESDocument , Serializable {

    private static final long serialVersionUID =  1L;

    //es id
    @Id
    @ESField(type = "keyword")
    private  String  logId;

    /**
     * 日志级别，用户可以自己解释这个属性
     */
    @ESField(type = "keyword")
    private String logLevel;

    /**
     * 操作用户
     */
    @ESField(type = "keyword")
    private String userCode;


    /**
     * 操作用户所属机构
     */
    @ESField(type = "keyword")
    private String unitCode;

    @ESField(type = "keyword")
    private String topUnit;
    /**
     * 同一个请求同一个 协作号，主要用于调试和跟踪
     */
    @ESField(type = "keyword")
    private String correlationId;

    /**
     * 操作时间
     */
    @ESField(type = "date")
    private Long optTime;

    /**
     * 业务系统编号
     */
    @ESField(type = "keyword")
    private String osId;

    /**
     * 操作业务编号
     */
    @ESField(type = "keyword")
    private String optId;

    /**
     * 业务操作方法
     */
    @ESField(type = "keyword")
    private String optMethod;

    /**
     * 业务对象组件，复合主键用&amp;连接格式与url参数类似
     */
    @ESField(type = "keyword")
    private String optTag;

    @ESField(type = "keyword")
    private String loginIp;

    /**
     * 日志内容描述; 也可以是json
     */
    @ESField(type = "text", query = true, highlight = true, analyzer = "ik_smart")
    private String optContent;

    /**
     * 更新前旧值，json格式，这个字段不是必须的
     */
    @ESField(type = "text", query = true, highlight = true, analyzer = "ik_smart")
    private String newValue;

    /**
     * 更新后新值，json格式，这个字段不是必须的
     */
    @ESField(type = "text", query = true, highlight = true, analyzer = "ik_smart")
    private String oldValue;


    @Override
    public String obtainDocumentId() {
        return this.logId;
    }

    @Override
    public JSONObject toJSONObject() {
        return (JSONObject) JSON.toJSON(this);
    }

    public static ESOperationLog fromOperationLog(OperationLog log, String logId){
        ESOperationLog esLog = new ESOperationLog();
        if (StringUtils.isBlank(logId)){
            esLog.setLogId(UUID.randomUUID().toString().replaceAll("-",""));
        }else {
            esLog.setLogId(logId);
        }
        esLog.setOptTime(log.getOptTime()==null?null:log.getOptTime().getTime());
        esLog.setCorrelationId(log.getCorrelationId());
        esLog.setLogLevel(log.getLogLevel());

        esLog.setNewValue(log.getNewValue()!=null? JSON.toJSONString(log.getNewValue()) : null);
        esLog.setOldValue(log.getOldValue()!=null? JSON.toJSONString(log.getOldValue()) : null);
        esLog.setOptContent(log.getOptContent());
        esLog.setOptId(log.getOptId());
        esLog.setOptMethod(log.getOptMethod());
        esLog.setOptTag(log.getOptTag());
        esLog.setUnitCode(log.getUnitCode());
        esLog.setUserCode(log.getUserCode());
        esLog.setOsId(log.getOsId());
        esLog.setTopUnit(log.getTopUnit());
        esLog.setLoginIp(log.getLoginIp());
        esLog.setCorrelationId(log.getCorrelationId());
        return esLog;
    }

    public OperationLog toOperationLog() {
        OperationLog log = new OperationLog();
        log.setLogLevel(this.logLevel);
        log.setUserCode(this.userCode);
        log.setOptTime(DatetimeOpt.castObjectToDate(this.optTime));
        log.setOptId(this.optId);
        log.setOptTag(this.optTag);
        log.setOptMethod(this.optMethod);
        log.setOptContent(this.optContent);
        log.setNewValue(StringUtils.isBlank(this.newValue)? null : JSON.parse(this.newValue));
        log.setOldValue(StringUtils.isBlank(this.oldValue)? null : JSON.parse(this.oldValue));
        log.setUnitCode(this.unitCode);
        log.setCorrelationId(this.correlationId);
        log.setLoginIp(this.loginIp);
        log.setTopUnit(this.topUnit);
        return log;
    }
}

