package com.centit.framework.system.po;

import com.centit.framework.core.dao.DictionaryMap;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * 系统操作日志
 */

@ApiModel(value="系统操作日志对象",description="系统操作日志对象 OptLog")
@Data
@Table(name = "F_OPT_LOG")
public class OptLog implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "LOG_ID")
    @ValueGenerator(strategy = GeneratorType.UUID22)
    private String logId;//原本是Long类型

    /**
     * 日志级别
     * 使用常量LEVEL_INFO和LEVEL_ERROR表示
     * 默认级别为LEVEL_INFO
     */
    @Column(name = "LOG_LEVEL")
    @NotNull(message = "字段不能为空")
    @Length(max = 2, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "日志级别 使用常量LEVEL_INFO和LEVEL_ERROR表示 默认级别为LEVEL_INFO",name = "logLevel")
    @ValueGenerator(strategy = GeneratorType.CONSTANT, occasion = GeneratorTime.NEW, value = OperationLog.LEVEL_INFO)
    private String logLevel;

    @Column(name = "USER_CODE")
    @NotNull(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    @DictionaryMap(fieldName="userName", value="userCode")
    @ApiModelProperty(value = "用户代码",name = "userCode",required = true)
    private String userCode;

    @OrderBy("desc")
    @Column(name = "OPT_TIME")
    @NotNull(message = "字段不能为空")
    @Temporal(TemporalType.TIMESTAMP)
    @ValueGenerator(strategy = GeneratorType.FUNCTION, value = "today()")
    private Date optTime;

    /**
     * 业务操作ID，如记录的是用户管理模块，optId=F_OPT_INFO表中操作用户管理模块业务的主键
     */
    @Column(name = "OPT_ID")
    @ValueGenerator(strategy = GeneratorType.CONSTANT, occasion = GeneratorTime.NEW, value = "system")
    @Length(max = 64, message = "字段长度不能大于{max}")
    @DictionaryMap(fieldName="optName", value="optId")
    private String optId;

    /**
     * 操作业务标记
     * 一般用于关联到业务主体
     */
    @Column(name = "OPT_TAG")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String optTag;

    /**
     * 操作方法
     * 方法，或者字段
     * 方法使用 P_OPT_LOG_METHOD... 常量表示
     */
    @Column(name = "OPT_METHOD")
    @Length(max = 64, message = "字段长度不能大于{max}")
    private String optMethod;

    /**
     * 操作内容描述
     */
    @Column(name = "OPT_CONTENT")
    @NotNull(message = "字段不能为空")
    private String optContent;

    /**
     * 新值; 用于新旧值对比，也可以用于其他解释
     */
    @Column(name = "NEW_VALUE")
    private String newValue;
    /**
     * 原值; 用于新旧值对比，也可以用于其他解释
     */
    @Column(name = "OLD_VALUE")
    private String oldValue;
    /**
     * 机构代码
     */
    @Column(name = "UNIT_CODE")
    private String unitCode;

    /**
     * 关联id
     */
    @Column(name = "CORRELATION_ID")
    private String correlationId;

    /**
     * default constructor
     */
    public OptLog() {
    }

    public static OptLog valueOf(OperationLog other) {
        OptLog log = new OptLog();
        log.logLevel = other.getLogLevel();
        log.userCode = other.getUserCode();
        log.optTime = other.getOptTime();
        log.optId = other.getOptId();
        log.optTag = other.getOptTag();
        if(log.optTag!=null && log.optTag.length()>200){
            log.optTag = log.optTag.substring(0,200);
        }
        log.optMethod = other.getOptMethod();
        log.optContent = other.getOptContent();
        log.newValue = other.getNewValue();
        log.oldValue = other.getOldValue();
        log.unitCode = other.getUnitCode();
        log.correlationId = other.getCorrelationId();
        return log;
    }

    public OperationLog toOperationLog() {
        OperationLog log = new OperationLog();
        log.setLogLevel(this.logLevel);
        log.setUserCode(this.userCode);
        log.setOptTime(this.optTime);
        log.setOptId(this.optId);
        log.setOptTag(this.optTag);
        log.setOptMethod(this.optMethod);
        log.setOptContent(this.optContent);
        log.setNewValue(this.newValue);
        log.setOldValue(this.oldValue);
        log.setUnitCode(this.unitCode);
        log.setCorrelationId(this.correlationId);
        return log;
    }
}
