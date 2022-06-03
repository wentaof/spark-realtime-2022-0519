package com.atguigu.publisher_realtime.bean;

import lombok.*;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/3 8:14
 * @Version 1.0.0
 * @Description TODO
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class NameValue {
    private String name;
    private Object value;
}
