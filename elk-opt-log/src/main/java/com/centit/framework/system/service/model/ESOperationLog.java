
package com.centit.framework.system.service.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.search.annotation.ESField;
import com.centit.search.annotation.ESType;
import com.centit.search.document.ESDocument;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Date;
import java.util.UUID;

@Data
@Entity
@ESType(indexName = "esoperationlog")
public class ESOperationLog implements ESDocument {

    //es id
    @Id
    @ESField(type = "text",index=true,query = true)
    private  String  logId;

    /**
     * 日志级别，用户可以自己解释这个属性
     */
    @ESField(type = "text",index=true,query = true)
    private String logLevel;


    /**
     * 操作用户
     */
    @ESField(type = "text",index=true,query = true)
    private String userCode;


    /**
     * 操作用户所属机构
     */
    @ESField(type = "text",index=true,query = true)
    private String unitCode;

    /**
     * 同一个请求同一个 协作号，主要用于调试和跟踪
     */
    @ESField(type = "text",index=true,query = true)
    private String correlationId;

    /**
     * 操作时间
     */
    @ESField(type = "date")
    @Temporal(TemporalType.TIMESTAMP)
    private Date optTime;

    /**
     * 操作业务编号
     */
    @ESField(type = "text",index=true,query = true)
    private String optId;

    /**
     * 业务操作方法
     */
    @ESField(type = "text", index = true)
    private String optMethod;

    /**
     * 业务对象组件，复合主键用&amp;连接格式与url参数类似
     */
    @ESField(type = "text", index = true)
    private String optTag;

    /**
     * 日志内容描述; 也可以是json
     */
    @ESField(type = "text", index = true, query = true, highlight = true, analyzer = "ik_smart")
    private String optContent;

    /**
     * 更新前旧值，json格式，这个字段不是必须的
     */
    @ESField(type = "text", index = true, query = true, highlight = true, analyzer = "ik_smart")
    private String newValue;

    /**
     * 更新后新值，json格式，这个字段不是必须的
     */
    @ESField(type = "text", index = true, query = true, highlight = true, analyzer = "ik_smart")
    private String oldValue;


    @Override
    public String obtainDocumentId() {
        return this.logId;
    }

    @Override
    public JSONObject toJSONObject() {
        return (JSONObject) JSON.toJSON(this);
    }

    public static ESOperationLog fromOperationLog(OperationLog log,String logId){
        ESOperationLog esLog = new ESOperationLog();
        if (StringUtils.isBlank(logId)){
            esLog.setLogId(UUID.randomUUID().toString().replaceAll("-",""));
        }else {
            esLog.setLogId(logId);
        }
        esLog.setOptTime(log.getOptTime());
        esLog.setCorrelationId(log.getCorrelationId());
        esLog.setLogLevel(log.getLogLevel());
        esLog.setNewValue(log.getNewValue());
        esLog.setOldValue(log.getOldValue());
        esLog.setOptContent(log.getOptContent());
        esLog.setOptId(log.getOptId());
        esLog.setOptMethod(log.getOptMethod());
        esLog.setOptTag(log.getOptTag());
        esLog.setUnitCode(log.getUnitCode());
        esLog.setUserCode(log.getUserCode());
        return esLog;
    }
}

