package com.atguigu.springboot.springbootdemo.mapper.impl;

import com.atguigu.springboot.springbootdemo.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/2 16:38
 * @Version 1.0.0
 * @Description TODO
 */
@Repository  //告诉spring是dao层
@Qualifier()
public class UserMapperImpl implements UserMapper {
    @Override
    public String getUserandPasswd() {
        return "login";
    }
}
