package com.atguigu.publisher_realtime.controller;

import com.atguigu.publisher_realtime.bean.NameValue;
import com.atguigu.publisher_realtime.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/3 8:17
 * @Version 1.0.0
 * @Description 控制层
 */
@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;
    /*
    * 日活分析
    * */
    @GetMapping("dauRealtime")
    public Map<String, Object> dauRealTime(@RequestParam("td") String td){
        Map<String, Object> stringObjectMap = publisherService.doDauRealtime(td);
        return stringObjectMap;
    }
    /**
     * 交易分析 - 按照类别(年龄、性别)统计
     *
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     */
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(
            @RequestParam("itemName")String itemName ,
            @RequestParam("date") String date ,
            @RequestParam("t") String t){
        List<NameValue>  results =  publisherService.doStatsByItem(itemName , date , t );
        return results;
    }


    /**
     *
     * 交易分析 - 明细
     * http://bigdata.gmall.com/detailByItem?date=2021-02-02&itemName=小米手机&pageNo=1&pageSize=20
     */
    @GetMapping("detailByItem")
    public Map<String, Object> detailByItem(@RequestParam("date") String date ,
                                            @RequestParam("itemName") String itemName ,
                                            @RequestParam(value ="pageNo" , required = false  , defaultValue = "1") Integer pageNo ,
                                            @RequestParam(value = "pageSize" , required = false , defaultValue = "20") Integer pageSize){
        Map<String, Object> results =  publisherService.doDetailByItem(date, itemName, pageNo, pageSize);
        return results ;
    }


}
