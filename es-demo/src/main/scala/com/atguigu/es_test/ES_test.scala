package com.atguigu.es_test

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/30 15:40
  * @Version 1.0.0
  * @Description TODO
  */
object ES_test {
  var client: RestHighLevelClient = create()
  var index:String = "movie_test_20210103"
  def create()={
    val builder: RestClientBuilder = RestClient.builder(new HttpHost("192.168.198.132",9200))
    client = new RestHighLevelClient(builder)
    client
  }

  def close()= {
    if (client != null) {
      client.close()
    }
  }


  def put(): Unit = {
    val request = new IndexRequest(index)
    val movie = Movie("1001","速度与激情11")
    val movie_json: String = JSON.toJSONString(movie,new SerializeConfig(true))
    request.source(movie_json,XContentType.JSON)
    request.id("1001") //指定id就是幂等作操, 不指定就是随机生成_id
    client.index(request,RequestOptions.DEFAULT)
  }

  def post() = {
// 这个就是put操作 区分幂等不幂等就是否指定id
    val indexRequest: IndexRequest = new IndexRequest()
    //指定索引
    indexRequest.index("movie1018")
    //指定doc
    val movie: Movie = Movie("1001","速度与激情1")
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson,XContentType.JSON)
    client.index(indexRequest , RequestOptions.DEFAULT)
  }

  def bulk() = {
    val bulkRequest: BulkRequest = new BulkRequest()
    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "水门桥"),
      Movie("1004", "狙击手"),
      Movie("1005", "熊出没")
    )
    for (elem <- movies) {
      val request = new IndexRequest(index)
      request.source(JSON.toJSONString(elem,new SerializeConfig(true)),XContentType.JSON)
      request.id(elem.id)
      bulkRequest.add(request)

    }
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

/*
单条修改
* */
  def update() = {
    val request = new UpdateRequest(index,"1001")
    request.doc("movieName","update")
    client.update(request,RequestOptions.DEFAULT)
  }

  def updateByQuery() = {
    //qyery
    val request = new UpdateByQueryRequest(index)
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    val termQueryBuilder: TermQueryBuilder = QueryBuilders.termQuery("movieName.keyword","熊出没")
    boolQueryBuilder.filter(termQueryBuilder)
    request.setQuery(boolQueryBuilder)
    //update
    val params = new util.HashMap[String,AnyRef]()
    params.put("newName","光头强")
    val script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx._source['movieName']=params.newName",
      params
    )
    request.setScript(script)
    client.updateByQuery(request,RequestOptions.DEFAULT)
  }

  def delete() = {
    val request = new DeleteRequest(index)
    request.id("1001")
    client.delete(request, RequestOptions.DEFAULT)
  }

  def getById() = {
    val request = new GetRequest(index,"1002")
    val response: GetResponse = client.get(request, RequestOptions.DEFAULT)
    val data_str: String = response.getSourceAsString
    println(data_str)
  }

  def searchByFilter(): Unit ={
    val searchRequest: SearchRequest = new SearchRequest(index)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //query
    //bool
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    //filter
    val rangeQueryBuilder: RangeQueryBuilder =
      QueryBuilders.rangeQuery("doubanScore").gte(5.0)
    boolQueryBuilder.filter(rangeQueryBuilder)
    //must
    val matchQueryBuilder: MatchQueryBuilder =
      QueryBuilders.matchQuery("name","red sea")
    boolQueryBuilder.must(matchQueryBuilder)
    searchSourceBuilder.query(boolQueryBuilder)

    //分页
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(1)
    //排序
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)

    //高亮
    val highlightBuilder: HighlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")
    searchSourceBuilder.highlighter(highlightBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      client.search(searchRequest , RequestOptions.DEFAULT)

    //获取总条数据
    val totalDocs: Long = searchResponse.getHits.getTotalHits.value

    //明细
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      //数据
      val dataJson: String = hit.getSourceAsString
      //hit.getSourceAsMap
      //提取高亮
      val highlightFields: util.Map[String, HighlightField] = hit.getHighlightFields
      val highlightField: HighlightField = highlightFields.get("name")
      val fragments: Array[Text] = highlightField.getFragments
      val highLightValue: String = fragments(0).toString

      println("明细数据: " +  dataJson)
      println("高亮: " + highLightValue)

    }
  }

  /**
    * 查询 - 聚合查询
    *
    * 查询每位演员参演的电影的平均分，倒叙排序
    */

  def searchByAggs(): Unit ={
    val searchRequest: SearchRequest = new SearchRequest(index)
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //不要明细
    searchSourceBuilder.size(0)
    //group
    val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders.terms("groupbyactorname").
      field("actorList.name.keyword").
      size(10).
      order(BucketOrder.aggregation("doubanscoreavg",false))
    //avg
    val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("doubanscoreavg").field("doubanScore")
    termsAggregationBuilder.subAggregation(avgAggregationBuilder)
    searchSourceBuilder.aggregation(termsAggregationBuilder)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse =
      client.search(searchRequest , RequestOptions.DEFAULT)
    val aggregations: Aggregations = searchResponse.getAggregations
    //val groupbyactornameAggregation: Aggregation =
    //    aggregations.get[Aggregation]("groupbyactorname")
    val groupbyactornameParsedTerms: ParsedTerms =
    aggregations.get[ParsedTerms]("groupbyactorname")
    val buckets: util.List[_ <: Terms.Bucket] = groupbyactornameParsedTerms.getBuckets
    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      //演员名字
      val actorName: String = bucket.getKeyAsString
      //电影个数
      val moviecount: Long = bucket.getDocCount

      //平均分
      val aggregations: Aggregations = bucket.getAggregations
      val doubanscoreavgParsedAvg: ParsedAvg =
        aggregations.get[ParsedAvg]("doubanscoreavg")
      val avgScore: Double = doubanscoreavgParsedAvg.getValue

      println(s"$actorName 共参演了 $moviecount 部电影， 平均分为 $avgScore")
    }
  }

  def main(args: Array[String]): Unit = {
//    println(client)
//    put()
//    bulk()
//    update()
//    updateByQuery()
//    delete()
//    getById()
//    searchByFilter()
    searchByAggs()
    close()
  }


}
case class Movie(id:String, movieName:String)