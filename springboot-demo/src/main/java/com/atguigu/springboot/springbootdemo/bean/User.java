package com.atguigu.springboot.springbootdemo.bean;

import lombok.*;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/2 15:37
 * @Version 1.0.0
 * @Description 忘记导lombok了, 先手写
 */
@ToString
@AllArgsConstructor
@Getter
@Setter
@NoArgsConstructor
public class User {
    private String name;
    private int age;
    private String address;

}
