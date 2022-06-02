package com.atguigu.springboot.springbootdemo.service.impl;

import com.atguigu.springboot.springbootdemo.service.UserService;
import org.springframework.stereotype.Service;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/2 16:44
 * @Version 1.0.0
 * @Description TODO
 */
@Service
public class UserSeriviceImple2 implements UserService {
    @Override
    public String Dologin() {
        return "UserSeriviceImple2";
    }
}
