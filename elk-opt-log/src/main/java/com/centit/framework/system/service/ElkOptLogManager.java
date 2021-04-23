package com.centit.framework.system.service;

import com.alibaba.fastjson.JSONObject;
import com.centit.framework.core.controller.SmartDateFormat;
import com.centit.framework.model.adapter.OperationLogWriter;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.system.po.ESOperationLog;
import com.centit.search.service.ESServerConfig;
import com.centit.search.service.Impl.ESIndexer;
import com.centit.search.service.IndexerSearcherFactory;
import com.centit.support.common.ObjectException;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.*;

@Service("elkOptLogManager")
public class ElkOptLogManager implements OperationLogWriter {
    public static final Logger logger = LoggerFactory.getLogger(ElkOptLogManager.class);

    @Autowired(required = false)
    private ESIndexer esObjectIndexer;

    @Value("${elasticsearch.index}")
    private String indexName;

    @Autowired
    private  ESServerConfig esServerConfig;

    @Override
    public void save(OperationLog operationLog) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,null);
        if (esObjectIndexer.saveNewDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    @Override
    public void save(List<OperationLog> optLogs) {
        GenericObjectPool<RestHighLevelClient> restHighLevelClientGenericObjectPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            restHighLevelClient = restHighLevelClientGenericObjectPool.borrowObject();
            BulkRequest requestBulk = new BulkRequest(indexName);
            for (OperationLog operationLog : optLogs) {
                String json = JSONObject.toJSONString(operationLog);
                String documentid =  UUID.randomUUID().toString().replaceAll("-","");
                IndexRequest indexReq = new IndexRequest().source(json, XContentType.JSON);
                indexReq.id(documentid);
                requestBulk.add(indexReq);
            }
            BulkResponse bulkResponse = restHighLevelClient.bulk(requestBulk, RequestOptions.DEFAULT);
            for(BulkItemResponse bulkItemResponse : bulkResponse){
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                logger.debug("单条返回结果："+indexResponse.toString());
                if(bulkItemResponse.isFailed()){
                    logger.error("es 返回错误:"+bulkItemResponse.getFailureMessage());
                }
            }
        }catch (Exception e){
            logger.error("批量插入异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient!=null){
                restHighLevelClientGenericObjectPool.returnObject(restHighLevelClient);
            }
        }
    }

    public void deleteObjectById(String docId) {
        if (!esObjectIndexer.deleteDocument(docId)) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    public void updateOperationLog(OperationLog operationLog,String logId) {
        ESOperationLog esOperationLog = ESOperationLog.fromOperationLog(operationLog,logId);
        if (esObjectIndexer.mergeDocument(esOperationLog) == null) {
            throw new ObjectException(500, "elasticsearch操作失败");
        }
    }

    /*
     * 日志精确查询
     * Map<String, Object> filterMap
     * 中为条件 sql ，key为字段，格式为 字段代码_表达式， 这个比较难实现
     * @see com.centit.support.database.jsonmaptable.buildFilterSql
     */
    @Override
    public List<? extends OperationLog> listOptLog(String optId, Map<String, Object> filterMap, int startPos, int maxRows){
        GenericObjectPool<RestHighLevelClient> restHighLevelClientGenericObjectPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            restHighLevelClient = restHighLevelClientGenericObjectPool.borrowObject();
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            publicbuild(filterMap,boolQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.explain(true);
            searchSourceBuilder.from((startPos>1)?(startPos-1)* maxRows:0);
            searchSourceBuilder.size(maxRows);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse= restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<OperationLog> result = new ArrayList<>();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                OperationLog esOperationLog = JSONObject.parseObject(sourceAsString, OperationLog.class);
                result.add(esOperationLog);
            }
            return result;
        }catch (Exception e){
            logger.error("查询异常,异常信息："+e.getMessage());
        }finally {
            if (restHighLevelClient!=null){
                restHighLevelClientGenericObjectPool.returnObject(restHighLevelClient);
            }
        }
        return null;
    }

    /*
     * 查询日志数量，这个不知道是否可以做到
     */
    @Override
    public int countOptLog(String optId, Map<String, Object> filter){
        GenericObjectPool<RestHighLevelClient> restHighLevelClientGenericObjectPool = IndexerSearcherFactory.obtainclientPool(esServerConfig);
        RestHighLevelClient restHighLevelClient=null;
        try {
            restHighLevelClient = restHighLevelClientGenericObjectPool.borrowObject();
            CountRequest countRequest = new CountRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            publicbuild(filter,boolQueryBuilder);
            searchSourceBuilder.query(boolQueryBuilder);
            countRequest.source(searchSourceBuilder);
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

    private void publicbuild( Map<String, Object> filter, BoolQueryBuilder boolQueryBuilder){
        if (filter.size()==0){
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        }else {
            filter.forEach((key,value)->{
                if (key.startsWith("optTime")){
                    try {
                        buildQuery(key,"optTime",value,boolQueryBuilder);
                    } catch (ParseException e) {
                        logger.error("转换日期异常，异常信息："+e.getMessage());
                    }
                }else if (key.startsWith("unitCode")){
                    List<Object> data=null;
                    if (value.getClass().isArray()){
                        data= Arrays.asList((Object[])value);
                    }
                    boolQueryBuilder.must(QueryBuilders.termsQuery(key,data));
                }else {
                    boolQueryBuilder.must(QueryBuilders.termQuery(key,value));
                }
            });
        }
    }

    private void buildQuery(String key, String field, Object value,BoolQueryBuilder boolQueryBuilder) throws ParseException {
        String optSuffix = key.substring(key.length() - 3).toLowerCase();
        long date = new SmartDateFormat("yyyy-MM-dd HH:mm:ss").parse(String.valueOf(value)).getTime();
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
                boolQueryBuilder.must(QueryBuilders.termQuery(field,date));
                break;
        }

    }
}
