package com.atguigu.publisher_realtime.service;

import com.atguigu.publisher_realtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/3 8:19
 * @Version 1.0.0
 * @Description TODO
 */
public interface PublisherService {
    //日活
    Map<String, Object> doDauRealtime(String td);

    List<NameValue> doStatsByItem(String itemName, String date, String t);

    Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize);

}
