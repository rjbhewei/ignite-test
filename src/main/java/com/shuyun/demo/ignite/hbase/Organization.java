package com.shuyun.demo.ignite.hbase;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/6  11:23
 * @desc
 */
@Getter
@Setter
@AllArgsConstructor
@ToString
public class Organization {

    private Long id;

    private String name;

    private String city;

}
