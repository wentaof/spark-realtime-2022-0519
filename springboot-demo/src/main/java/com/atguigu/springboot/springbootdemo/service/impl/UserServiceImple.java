package com.atguigu.springboot.springbootdemo.service.impl;

import com.atguigu.springboot.springbootdemo.mapper.UserMapper;
import com.atguigu.springboot.springbootdemo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/2 16:32
 * @Version 1.0.0
 * @Description TODO
 */
//@Service //告诉spring这个类是service层的
@Service("impl1")
public class UserServiceImple implements UserService {
    /*
     * 调用dao
     * */
    @Autowired
    UserMapper userMapper;

    @Override
    public String Dologin() {
        return userMapper.getUserandPasswd();
    }
}
