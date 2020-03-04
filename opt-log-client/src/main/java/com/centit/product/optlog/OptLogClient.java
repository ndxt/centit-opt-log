package com.centit.product.optlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.appclient.AppSession;
import com.centit.framework.appclient.HttpReceiveJSON;
import com.centit.framework.appclient.RestfulHttpRequest;
import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.support.common.ObjectException;
import com.centit.support.network.UrlOptUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Service
public class OptLogClient implements OperationLogWriter {
    private static final Logger logger = LoggerFactory.getLogger(OptLogClient.class);

    @Value("${optlog.server.url:}")
    private String optlogUrl;

    @Value("${optlog.server.loginUrl:}")
    private String optlogLoginUrl;

    @Value("${optlog.server.username:}")
    private String optlogUser;

    @Value("${optlog.server.password:}")
    private String optlogPassword;

    protected AppSession appSession;

    @PostConstruct
    public void init(){
        appSession = new AppSession();
        appSession.setAppServerUrl(optlogUrl);
        if(StringUtils.isNotBlank(optlogLoginUrl)){
            appSession.setAppLoginUrl(optlogLoginUrl);
        }
        appSession.setNeedAuthenticated(StringUtils.isNotBlank(optlogUser)
            && StringUtils.isNotBlank(optlogPassword) );
        appSession.setUserCode(optlogUser);
        appSession.setPassword(optlogPassword);
    }

    @Override
    public void save(OperationLog optLog) {
        String flowJson = RestfulHttpRequest.jsonPost(appSession,
            "/optlog", optLog);
        HttpReceiveJSON receiveJSON = HttpReceiveJSON.valueOfJson(flowJson);
        //RestfulHttpRequest.checkHttpReceiveJSON(receiveJSON);
        if(receiveJSON == null ||receiveJSON.getCode() != 0){
            logger.error("日志写入错误，optlog: " + optLog.toString());
        }
    }

    @Override
    public void save(List<OperationLog> optLogs) {
        String flowJson = RestfulHttpRequest.jsonPost(appSession,
            "/optlog/saveMany", optLogs);
        HttpReceiveJSON receiveJSON = HttpReceiveJSON.valueOfJson(flowJson);
        //RestfulHttpRequest.checkHttpReceiveJSON(receiveJSON);
        if(receiveJSON == null ||receiveJSON.getCode() != 0){
            logger.error("日志写入错误，optlogs: " + JSON.toJSONString(optLogs));
        }
    }

    @Override
    public List<? extends OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows) {
        filterMap.put("startPos", startPos);
        filterMap.put("maxRows", maxRows);
        HttpReceiveJSON receiveJSON = RestfulHttpRequest.getResponseData(appSession,
                UrlOptUtils.appendParamsToUrl("/optlog/query/" + optId,
                    filterMap));
        RestfulHttpRequest.checkHttpReceiveJSON(receiveJSON);
        return receiveJSON.getDataAsArray(OperationLog.class);
    }

    @Override
    public int countOptLog(String optId, Map<String, Object> filterMap) {
        HttpReceiveJSON receiveJSON = RestfulHttpRequest.getResponseData(appSession,
            UrlOptUtils.appendParamsToUrl("/optlog/count/" + optId,
                filterMap));
        RestfulHttpRequest.checkHttpReceiveJSON(receiveJSON);
        return receiveJSON.getDataAsObject(Integer.class);
    }
}
