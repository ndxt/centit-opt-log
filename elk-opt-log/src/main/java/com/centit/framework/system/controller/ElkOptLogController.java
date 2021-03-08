package com.centit.framework.system.controller;

import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.service.ElkOptLogManager;
import com.centit.search.service.Impl.ESSearcher;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/elkoptlog")
@Api(tags = "elk日志操作控制器")
public class ElkOptLogController extends BaseController {

    @Resource
    ElkOptLogManager elkOptLogManager;

    @Autowired(required = false)
    private ESSearcher esObjectSearcher;

    /**
     * 单条日志新增
     * @return
     */
    @ApiOperation(value = "单条新增日志信息")
    @RequestMapping(value = "/createOperationLog", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public void createOperationLog(@RequestBody OperationLog operationLog) throws IOException {
        elkOptLogManager.save(operationLog);
    }
    /**
     * 批量日志新增
     * @return
     */
    @ApiOperation(value = "批量新增日志信息")
    @RequestMapping(value = "/batchCreateOperationLog", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public void batchCreateOperationLog(@RequestBody List<OperationLog> operationLogs) throws IOException {
        operationLogs.forEach(esOperationLog ->{
            elkOptLogManager.save(esOperationLog);
        } );
    }

    /**
     * 删除
     * @param logId questionId
     */
    @ApiOperation(value = "删除日志信息")
    @RequestMapping(value = "/deleteOperationLog/{logId}", method = {RequestMethod.DELETE})
    @WrapUpResponseBody
    public void deleteQuestionCatalog(@PathVariable String logId) {
        elkOptLogManager.deleteObjectById(logId);
    }



    /**
     * 修改
     */
    @ApiOperation(value = "修改日志信息")
    @RequestMapping(value = "/updateOperationLog/{logId}", method = {RequestMethod.PUT})
    @WrapUpResponseBody
    public void updateOperationLog(@RequestBody OperationLog operationLog,@PathVariable String logId) {
        elkOptLogManager.updateOperationLog(operationLog,logId);
    }

    /**
     *
     * @param map   字段名
     * @param value  字段值  （分词后值）
     * @param queryWord  在根据前面字段和字段值过滤后再进行结果的筛选
     * @param pageDesc  分页
     * @return
     */
    @ApiOperation(value = "精确查询日志信息")
    @RequestMapping(value = "/listES/{map}/{value}/{queryWord}", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult<Map<String, Object>> listEs(String map, String value, String queryWord, PageDesc pageDesc) {
        Pair<Long, List<Map<String, Object>>> res =
            esObjectSearcher.search(CollectionsOpt.createHashMap(map, value),queryWord, pageDesc.getPageNo(), pageDesc.getPageSize());
        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(res.getLeft()));
        return PageQueryResult.createResult(res.getRight(), pageDesc);
    }



    @ApiOperation(value = "模糊查询日志信息（不填关键字默认查询全部）")
    @RequestMapping(value = "/listESall", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult<Map<String, Object>> listEsAll(String queryWord, PageDesc pageDesc) {
        Pair<Long, List<Map<String, Object>>> res =
            esObjectSearcher.search(queryWord, pageDesc.getPageNo(), pageDesc.getPageSize());
        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(res.getLeft()));
        return PageQueryResult.createResult(res.getRight(), pageDesc);
    }

}
