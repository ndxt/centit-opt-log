package com.centit.framework.system.controller;

import com.alibaba.fastjson2.JSONArray;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.ResponseMapData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.operationlog.RecordOperationLog;
import com.centit.framework.system.service.OperationLogManager;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Controller
@Api(value = "系统日志维护接口", tags = "系统日志操作接口")
@RequestMapping(value = "/optlog")
public class OptLogController extends BaseController {

    @Autowired
    @NotNull
    private OperationLogManager optLogManager;

    /*
     * 查询系统日志
     * @param field    需要显示的字段
     * @param pageDesc 分页信息
     * @param request  HttpServletRequest
     */
    @ApiOperation(value = "查询系统日志列表", notes = "查询系统日志列表。")
    @ApiImplicitParams({
        @ApiImplicitParam(
            name = "field", value = "指需要显示的属性名",
            allowMultiple = true, paramType = "query", dataType = "String"),
        @ApiImplicitParam(
            name = "pageDesc", value = "分页对象",
            paramType = "query", dataTypeClass = PageDesc.class)
    })
    @GetMapping
    @WrapUpResponseBody
    public ResponseMapData list(String[] field, PageDesc pageDesc, HttpServletRequest request) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        String topUnit = StringBaseOpt.castObjectToString(searchColumn.get("topUnit"));
        if(StringUtils.isBlank(topUnit)) {
            topUnit = WebOptUtils.getCurrentTopUnit(request);
            if(StringUtils.isBlank(topUnit)) {
                throw new ObjectException(ResponseData.ERROR_FIELD_INPUT_NOT_VALID,
                    getI18nMessage("error.701.field_is_blank",  request,"topUnit"));
            }
            searchColumn.put("topUnit", topUnit);
        }
        JSONArray jsonArray = optLogManager.listOptLogsAsJson(field, searchColumn, pageDesc);
        ResponseMapData resData = new ResponseMapData();
        resData.addResponseData(PageQueryResult.OBJECT_LIST_LABEL, jsonArray);
        resData.addResponseData(PageQueryResult.PAGE_INFO_LABEL, pageDesc);
        resData.addResponseData(CodeBook.SELF_ORDER_BY, searchColumn.get(CodeBook.SELF_ORDER_BY));
        return resData;
    }

    /*
     * 查询单条日志
     * @param logId logId
     */
    @ApiOperation(value = "查询单条日志", notes = "根据日志id查询单条日志。")
    @ApiImplicitParam(
        name = "logId", value = "日志id",
        required = true, paramType = "query", dataType = "String")
    @RequestMapping(value = "/{logId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public ResponseData getOptLogById(@PathVariable String logId) {
        OperationLog operationLog = optLogManager.getOptLogById(logId);
        if (null == operationLog) {
            return ResponseData.makeErrorMessage("日志信息不存在");
        }
        return ResponseData.makeResponseData(DictionaryMapUtils.objectToJSON(operationLog));
    }

    /*
     * 删除单条系统日志
     * @param logId logId
     */
    @ApiOperation(value = "删除单条系统日志", notes = "根据日志id删除单条日志。")
    @ApiImplicitParam(
        name = "logId", value = "日志id",
        required = true, paramType = "query", dataType = "String")
    @RequestMapping(value = "/{logId}", method = {RequestMethod.DELETE})
    @RecordOperationLog(content = "操作IP地址:{loginIp},用户{loginUser.userName}删除日志")
    @WrapUpResponseBody
    public ResponseData deleteOne(@PathVariable String logId) {
        optLogManager.deleteOptLogById(logId);
        return ResponseData.successResponse;
    }

    /*
     * 删除多条系统日志
     * @param logIds logIds[]
     */
    @ApiOperation(value = "删除多条系统日志", notes = "删除多条系统日志。")
    @ApiImplicitParam(
        name = "logIds", value = "数组格式，多个日志ID", required = true,
        allowMultiple = true, paramType = "query", dataType = "String")
    @RequestMapping(value = "/deleteMany", method = RequestMethod.DELETE)
    @RecordOperationLog(content = "操作IP地址:{loginIp},用户{loginUser.userName}删除日志")
    @WrapUpResponseBody
    public ResponseData deleteMany(String[] logIds) {
        optLogManager.deleteMany(logIds);
        return ResponseData.successResponse;
    }

    /*
     * 删除某时段之前的系统日志
     * @param beginDate Date
     */
    @ApiOperation(value = "删除某时段之前的系统日志", notes = "删除某时段之前的系统日志。")
    @RequestMapping(value = "/delete/{beginDate}", method = {RequestMethod.DELETE})
    @RecordOperationLog(content = "操作IP地址:{loginIp},用户{loginUser.userName}删除日志")
    @WrapUpResponseBody
    public ResponseData deleteByTime(@PathVariable String beginDate) {
        if(StringBaseOpt.isNvl(beginDate)){
            return ResponseData.makeErrorMessage("请指定具体的删除时间范围！");
        }
        int delete = optLogManager.delete(beginDate);
        return ResponseData.makeSuccessResponse(StringBaseOpt.castObjectToString(delete));
    }
    //暂时把saveOne在swagger中暴露出来
    @RequestMapping(method = RequestMethod.POST)
    @WrapUpResponseBody
    public void saveOne(@RequestBody OperationLog optLog) {
        optLogManager.save(optLog);
    }

    @RequestMapping(value = "/saveMany",method = RequestMethod.POST)
    @WrapUpResponseBody
    public void saveMany(@RequestBody String optLogJsonArray) {
        List<OperationLog> optlogs = JSONArray.parseArray(optLogJsonArray, OperationLog.class);
        optLogManager.save(optlogs);
    }

    @ApiOperation(value = "按应用查询日志", notes = "按应用查询日志。")
    @ApiImplicitParams({
        @ApiImplicitParam(
            name = "optId", value = "业务系统名",
            allowMultiple = true, paramType = "path", dataType = "String"),
        @ApiImplicitParam(
            name = "startPos", value = "分页查询起始页面",
            paramType = "query", dataTypeClass = Integer.class),
        @ApiImplicitParam(
            name = "maxSize", value = "每页大小",
            paramType = "query", dataTypeClass = Integer.class)
    })
    @GetMapping("/query/{optId}")
    @WrapUpResponseBody
    public List<OperationLog> queryOptlog(@PathVariable String optId,
                                                     Integer startPos,
                                                     Integer maxSize,
                                                     HttpServletRequest request) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        searchColumn.remove("startPos");
        searchColumn.remove("maxSize");
        return optLogManager.listOptLog(optId, searchColumn, startPos, maxSize);
    }

    @ApiOperation(value = "按应用查询日志条数", notes = "按应用查询日志条数。")
    @ApiImplicitParam(
        name = "optId", value = "业务系统名",
        allowMultiple = true, paramType = "path", dataType = "String")
    @GetMapping("/count/{optId}")
    @WrapUpResponseBody
    public Integer countOptlog(String optId, HttpServletRequest request) {
        Map<String, Object> searchColumn = BaseController.collectRequestParameters(request);
        return optLogManager.countOptLog(optId, searchColumn);
    }

   /* @ApiOperation(value = "精确查询日志信息")
    @RequestMapping(value = "/listES/{map}/{value}/{queryWord}", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult<Map<String, Object>> listEs(String map, String value, String queryWord, PageDesc pageDesc) {
        Pair<Long, List<Map<String, Object>>> res =
            elkOptLogSearcher.search(CollectionsOpt.createHashMap(map, value),queryWord, pageDesc.getPageNo(), pageDesc.getPageSize());
        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(res.getLeft()));
        return PageQueryResult.createResult(res.getRight(), pageDesc);
    }

    @ApiOperation(value = "模糊查询日志信息（不填关键字默认查询全部）")
    @RequestMapping(value = "/listESall", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult<Map<String, Object>> listEsAll(String queryWord, PageDesc pageDesc) {
        Pair<Long, List<Map<String, Object>>> res =
            elkOptLogSearcher.search(queryWord, pageDesc.getPageNo(), pageDesc.getPageSize());
        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(res.getLeft()));
        return PageQueryResult.createResult(res.getRight(), pageDesc);
    }*/
}
