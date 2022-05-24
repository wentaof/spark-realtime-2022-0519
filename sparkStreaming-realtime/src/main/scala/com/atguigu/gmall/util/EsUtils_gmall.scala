package com.atguigu.gmall.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall.bean.DauInfo
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.cluster.metadata.Metadata.XContentContext
import org.elasticsearch.common.xcontent.{XContent, XContentType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.mutable.ListBuffer

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/24 22:59
  * @Version 1.0.0
  * @Description ES 工具类
  */
object EsUtils_gmall {

  //  客户端对象
  val esClient: RestHighLevelClient = build()
//创建es客户端对象
  def build(): RestHighLevelClient = {
    val host = PropertiesUtils(MyConfig.ES_HOST)
    val port = PropertiesUtils(MyConfig.ES_PORT)
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host,port.toInt))
    val client = new RestHighLevelClient(restClientBuilder)
    client
  }

  def bulkSave(indexName: String, docs: List[(String, AnyRef)]) = {
    val bulkRequest = new BulkRequest(indexName)
    for((docId, docObj) <- docs){
      val request = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      request.source(dataJson,XContentType.JSON)
      request.id(docId)
      bulkRequest.add(request)
    }
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
    * 查询指定的字段
    */
  def searchField(indexName: String, fieldName: String): List[String] = {
    //判断索引是否存在
    val getIndexRequest: GetIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean =
      esClient.indices().exists(getIndexRequest,RequestOptions.DEFAULT)
    if(!isExists){
      return null
    }
    //正常从ES中提取指定的字段
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest: SearchRequest = new SearchRequest(indexName)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchSource(fieldName,null).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      esClient.search(searchRequest , RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceMap= hit.getSourceAsMap
      val mid: String = sourceMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }

  def close()={
    if(esClient != null){
      esClient.close()
    }
  }
}
