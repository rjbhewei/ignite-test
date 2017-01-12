package com.shuyun.demo.test.hbase;

import com.hewei.helper.hbase.utils.HbaseHelper;
import com.shuyun.demo.ignite.hbase.Organization;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/6  13:12
 * @desc
 */
public class TestOrganization {

    private static final String ZK_URL = "172.18.2.121:2181,172.18.2.40:2181,172.18.2.39:2181";

    private static final HbaseHelper HELPER = new HbaseHelper(ZK_URL);

    private static final String TABLE_NAME = "test.organization";

    private static final String FAMILY_NAME = "orgFamilyName";

    private static final String NAME = "name";

    private static final String CITY = "city";

    private static final byte[] NAME_COLUMN = Bytes.toBytes(NAME);

    private static final byte[] CITY_COLUMN = Bytes.toBytes(CITY);

    public static void main(String[] args) throws Exception {

        //HELPER.deleteTable(TABLE_NAME);
        //
        //HELPER.createTable(TABLE_NAME, new String[]{FAMILY_NAME});

        List<Put> putList = new ArrayList<>();

        byte[] FAMILY = Bytes.toBytes(FAMILY_NAME);

        for(int i = 1; i <= 10; i++) {
            putList.add(new Put(Bytes.toBytes(i + ""))
                    .addColumn(FAMILY, NAME_COLUMN, Bytes.toBytes("hewei--" + i))
                    .addColumn(FAMILY, CITY_COLUMN, Bytes.toBytes("湖南--" + i)));
        }

        HELPER.addRows(TABLE_NAME, putList);

        List<Organization> orgs = new ArrayList<>();

        for(int i = 1; i <= 10; i++) {

            String name =HELPER.getValue(TABLE_NAME, i + "", FAMILY_NAME, NAME);

            String city=HELPER.getValue(TABLE_NAME, i + "", FAMILY_NAME, CITY);

            orgs.add(new Organization((long)i,name,city));
        }

        for(int i=0;i<orgs.size();i++){

            System.out.println(orgs.get(i));

        }


        //HELPER.deleteTable(TABLE_NAME);
    }
}
