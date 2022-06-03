package com.atguigu.publisher_realtime.mapper;

import com.atguigu.publisher_realtime.bean.NameValue;

import java.util.List;
import java.util.Map;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/3 8:20
 * @Version 1.0.0
 * @Description TODO
 */
public interface PublisherMapper {
    Map<String, Object> searchDau(String td);

    List<NameValue> searchStatsByItem(String itemName, String date, String field);

    Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize);

}
