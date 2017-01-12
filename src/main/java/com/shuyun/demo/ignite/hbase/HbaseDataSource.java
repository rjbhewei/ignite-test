package com.shuyun.demo.ignite.hbase;

import com.hewei.helper.hbase.utils.HbaseHelper;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/7  17:02
 * @desc
 */
public class HbaseDataSource {

    private static class HEWEI {

        private static final String ZK_URL = "172.18.2.121:2181,172.18.2.40:2181,172.18.2.39:2181";

        private static final HbaseHelper HELPER = new HbaseHelper(ZK_URL);
    }

    public static HbaseHelper dataSource() {
        return HEWEI.HELPER;
    }
}
