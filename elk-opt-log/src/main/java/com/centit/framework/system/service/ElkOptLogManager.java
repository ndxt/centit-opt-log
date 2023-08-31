package com.centit.framework.system.service;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.controller.SmartDateFormat;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.po.ESOperationLog;
import com.centit.search.document.DocumentUtils;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.Impl.ESIndexer;
import com.centit.search.service.Impl.ESSearcher;
import com.centit.search.service.IndexerSearcherFactory;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("elkOptLogManager")
public class ElkOptLogManager implements OperationLogManager {

    public static final Logger logger = LoggerFactory.getLogger(ElkOptLogManager.class);

    private ESIndexer elkOptLogIndexer;

    private ESSearcher elkOptLogSearcher;

    private ESServerConfig esServerConfig;

    public ElkOptLogManager(ESServerConfig esServerConfig) {
        this.esServerConfig =esServerConfig;
        this.elkOptLogIndexer = IndexerSearcherFactory.obtainIndexer(esServerConfig, ESOperationLog.class);
        this.elkOptLogSearcher = IndexerSearcherFactory.obtainSearcher(esServerConfig,ESOperationLog.class);
    }


    @Override
    public void save(OperationLog operationLog) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,null);
        if (elkOptLogIndexer.saveNewDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    @Override
    public void save(List<OperationLog> optLogs) {
        if (optLogs != null && optLogs.size() > 0){
            for (OperationLog optLog : optLogs) {
                save(optLog);
            }
        }
    }

    @SneakyThrows
    @Override
    public List<OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        publicbuild(optId,filterMap,boolQueryBuilder);
        Pair<Long, List<Map<String, Object>>> longListPair = elkOptLogSearcher.esSearch(boolQueryBuilder, startPos, maxRows);
        List<OperationLog> operationLogList = new ArrayList<>();
        if (longListPair != null){
            for (Map<String, Object> objectMap : longListPair.getValue()) {
                OperationLog operationLog = JSONObject.parseObject(StringBaseOpt.castObjectToString(objectMap),OperationLog.class);
                operationLogList.add(operationLog);
            }
        }
        return  operationLogList;
    }

    /*
     * 查询日志数量
     */
    @Override
    public int countOptLog(String optId, Map<String, Object> filter){
        GenericObjectPool<RestHighLevelClient> restHighLevelClientGenericObjectPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            String indexName = DocumentUtils.obtainDocumentIndexName(ESOperationLog.class);
            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            publicbuild(optId,filter,boolQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
            countRequest.source(searchSourceBuilder);
            restHighLevelClient = restHighLevelClientGenericObjectPool.borrowObject();
            CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
            return (int)countResponse.getCount();
        }catch (Exception e){
            logger.error("统计数量异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient!=null){
                restHighLevelClientGenericObjectPool.returnObject(restHighLevelClient);
            }
        }
        return 0;
    }

    @Override
    public OperationLog getOptLogById(String logId) {
        GenericObjectPool<RestHighLevelClient> clientPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient = null;
        try {
            String indexName = DocumentUtils.obtainDocumentIndexName(ESOperationLog.class);
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            TermQueryBuilder termQuery = QueryBuilders.termQuery("logId", logId);
            searchSourceBuilder.query(termQuery);
            searchRequest.source(searchSourceBuilder);
            restHighLevelClient = clientPool.borrowObject();
            SearchResponse searchResponse= restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            String sourceAsString = hits[0].getSourceAsString();
            OperationLog operationLog = JSONObject.parseObject(sourceAsString, OperationLog.class);
            operationLog.setLogId(hits[0].getId());
            return operationLog;
        }catch (Exception e){
            logger.error("查询异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient != null){
                clientPool.returnObject(restHighLevelClient);
            }
        }
        return null;
    }

    @Override
    public void deleteOptLogById(String logId) {
        if (!elkOptLogIndexer.deleteDocument(logId)) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    @Override
    public void deleteMany(String[] logIds) {
        if (logIds != null){
            for (String logId : logIds) {
                deleteObjectById(logId);
            }
        }
    }

    @Override
    public JSONArray listOptLogsAsJson(String[] fields, Map<String, Object> filterMap, PageDesc pageDesc) {
        GenericObjectPool<RestHighLevelClient> clientPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            String indexName = DocumentUtils.obtainDocumentIndexName(ESOperationLog.class);
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (fields != null && fields.length > 0){
                searchSourceBuilder.fetchSource(fields,null);
            }
            publicbuild(null,filterMap,boolQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.explain(true);
            searchSourceBuilder.from((pageDesc.getPageNo()>1)?(pageDesc.getPageNo()-1)* pageDesc.getPageSize():0);
            searchSourceBuilder.size(pageDesc.getPageSize());
            searchRequest.source(searchSourceBuilder);
            restHighLevelClient = clientPool.borrowObject();
            SearchResponse searchResponse= restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            JSONArray result = new JSONArray();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                OperationLog esOperationLog = JSONObject.parseObject(sourceAsString, OperationLog.class);
                esOperationLog.setLogId(hit.getId());
                JSONObject jsonObject= (JSONObject) JSON.toJSON(esOperationLog);
                jsonObject.put("userName", CodeRepositoryUtil.getValue("userCode",esOperationLog.getUserCode(),"all","zh_CN"));
                result.add(jsonObject);
            }
            //查询总条数
            SearchSourceBuilder sourceBuilderCount = new SearchSourceBuilder();
            CountRequest countRequest = new CountRequest(indexName);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            publicbuild(null,filterMap,boolQuery);
            sourceBuilderCount.query(boolQuery);
            countRequest.source(sourceBuilderCount);
            CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);
            pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(countResponse.getCount()));
            return result;
        }catch (Exception e){
            logger.error("查询异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient!=null){
                clientPool.returnObject(restHighLevelClient);
            }
        }
        return null;
    }

    @Override
    public int delete(String beginDate) {
        if(StringUtils.isBlank(beginDate)) {
            throw new ObjectException("请指定具体时间！");
        }
        GenericObjectPool<RestHighLevelClient> restHighLevelClientGenericObjectPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            restHighLevelClient = restHighLevelClientGenericObjectPool.borrowObject();
            String indexName = DocumentUtils.obtainDocumentIndexName(ESOperationLog.class);
            DeleteByQueryRequest delete = new DeleteByQueryRequest(indexName);
            Long date = new SmartDateFormat("yyyy-MM-dd HH:mm:ss").parse(beginDate).getTime();
            RangeQueryBuilder optTime = QueryBuilders.rangeQuery("optTime").lte(date);
            delete.setQuery(optTime);
            BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.deleteByQuery(delete, RequestOptions.DEFAULT);
            int batches = bulkByScrollResponse.getBatches();
            return batches;
        }catch (Exception e){
            logger.error("删除异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient!=null){
                restHighLevelClientGenericObjectPool.returnObject(restHighLevelClient);
            }
        }
        return 0;
    }

    public void deleteObjectById(String docId) {
        if (!elkOptLogIndexer.deleteDocument(docId)) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    public void updateOperationLog(OperationLog operationLog,String logId) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,logId);
        if (elkOptLogIndexer.mergeDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    private  void publicbuild(String optId,Map<String, Object> filter, BoolQueryBuilder boolQueryBuilder) throws ParseException {
        if (StringUtils.isNotBlank(optId)){
            boolQueryBuilder.must(QueryBuilders.termQuery("optId",optId));
        }
        removeField(ESOperationLog.class,filter);
        if (filter == null ||  filter.size() == 0){
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }else {
            for (Map.Entry<String, Object> entry : filter.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (StringUtils.isNotBlank(key) && value != null){
                    if (key.startsWith("optTime")){
                        buildQuery(key,"optTime",value,boolQueryBuilder);
                    }else if ("optContent".equals(key)){
                        boolQueryBuilder.must(QueryBuilders.matchQuery(key,value));
                    }else {
                        boolQueryBuilder.must(QueryBuilders.termQuery(key,value));
                    }
                }
            }
        }
    }

    private void buildQuery(String key, String field, Object value,BoolQueryBuilder boolQueryBuilder) throws ParseException {
        String optSuffix = key.substring(key.length() - 3).toLowerCase();
        Long date = new SmartDateFormat("yyyy-MM-dd HH:mm:ss").parse(String.valueOf(value)).getTime();
        switch (optSuffix) {
            case "_gt":
                boolQueryBuilder.must(QueryBuilders.rangeQuery(field).gt(date));
                break;
            case "_ge":
                boolQueryBuilder.must(QueryBuilders.rangeQuery(field).gte(date));
                break;
            case "_lt":
                boolQueryBuilder.must(QueryBuilders.rangeQuery(field).lt(date));
                break;
            case "_le":
                boolQueryBuilder.must(QueryBuilders.rangeQuery(field).lte(date));
                break;
            default:
                boolQueryBuilder.must(QueryBuilders.matchQuery(field,date));
                break;
        }

    }

    //移除非索引字段   比如：pageSize   pageNo  等。
    private void removeField(Class clzz,Map<String,Object> map){
        if (clzz == null || map == null ) {
            return;
        }
        List<String> fieldNames = Arrays.stream(clzz.getDeclaredFields()).map(Field::getName).collect(Collectors.toList());
        map.keySet().removeIf(key -> !fieldNames.contains(key) && !key.startsWith("optTime_"));
    }
}
