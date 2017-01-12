package com.shuyun.demo.ignite.jdbc;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author hewei
 * @version 5.0
 * @date 20174  14:25
 * @desc
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
public class Person {
    private long id;

    private long orgId;

    private String name;

    private int salary;

}
