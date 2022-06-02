package com.atguigu.springboot.springbootdemo.controller;

import com.atguigu.springboot.springbootdemo.bean.User;
import com.atguigu.springboot.springbootdemo.mapper.UserMapper;
import com.atguigu.springboot.springbootdemo.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.jws.soap.SOAPBinding;
import javax.websocket.server.PathParam;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/5/31 20:17
 * @Version 1.0.0
 * @Description TODO
 */
@RestController // = @Controller + @ResponseBody
//@Controller
@Slf4j
public class CustormController {
    @RequestMapping(value = "hello", method = RequestMethod.GET)
    //@ResponseBody
    public String hello() {
        return "hello";
    }

    @RequestMapping("helloworld")
    public String helloword() {
        return "hello world";
    }

    //    获取url中携带的参数
//   默认如果参数名和请求的url中参数名称相同就会默认赋值
//    http://localhost:8080/getParamFromURL?name=liuxiaoshuai&age=28
    @RequestMapping("getParamFromURL")
    public String getParamFromURL(String name, int age) {
        return "name: " + name + "    age: " + age;
    }

    //http://localhost:8080/getParamFromURL2?name=liuxiaoshuai&age=28
    //url请求中的参数和拦截中的参数不一致, 需要使用requstparam参数指定参数名称
    @RequestMapping("getParamFromURL2")
    public String getParamFromURL2(@RequestParam("name") String name2, @RequestParam("age") int age2) {
        return "name2: " + name2 + "    age2: " + age2;
    }


    /*
     * 拦截url中的参数, url路径中携带的参数
     *http://localhost:8080/getParamFromPath/fengwentao/18
     * http://localhost:8080/getParamFromPath/fengwentao/18?address=beijing
     * defaultValue 默认值, 如果不加, 请求的时候会报400的错误
     * */
    @RequestMapping("getParamFromPath/{username}/{age}")
    public String getParamFromPath(
            @PathVariable("username") String name,
            @PathVariable("age") String age,
            @RequestParam(value = "address", defaultValue = "天安门") String addr
    ) {
        return "getParamFromPath: name=" + name + " age: " + age + " address:" + addr;
    }

    /*
     * 获取封装在请求体中的参数
     *
     * */
    @RequestMapping("getParamFromPOJO")
    public User getParamFromPOJO(@RequestBody User user) {
        System.out.println(user.getName());
        log.info(user.getName());
        return user;
    }

    /*
     * 调用service
     * */
    @Autowired
    @Qualifier("impl1")   //默认是类名首字母小写   userSeriviceImple2 , 也可以通过@service("impl1") 指定
    UserService userService;

    @RequestMapping("dologin")
    public String dologin() {
        return userService.Dologin();
    }


}
