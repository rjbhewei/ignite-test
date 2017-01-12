package com.shuyun.demo.ignite.hbase;

import com.hewei.helper.hbase.utils.HbaseHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/6  11:24
 * @desc
 */
@Slf4j
public class OrganizationStore extends CacheStoreAdapter<Long, Organization> {

    private HbaseHelper HELPER = HbaseDataSource.dataSource();

    private static final String TABLE_NAME = "test.organization";

    private static final String FAMILY_NAME = "orgFamilyName";

    private static final String NAME = "name";

    private static final String CITY = "city";

    @Override
    public void loadCache(IgniteBiInClosure<Long, Organization> clo, Object... args) {
        System.out.println(">> Loading cache from store...");
    }

    @Override
    public Map<Long, Organization> loadAll(Iterable<? extends Long> keys) {

        if (keys == null || !keys.iterator().hasNext())
            return new HashMap<>();

        log.info(">> Map<Long, Organization> loadAll(Iterable<? extends Long> keys) from store...");

        Map<Long, Organization> loaded = new HashMap<>();

        for(Long key : keys) {

            log.info("hbase load key : {}" , key);

            String name = null, city = null;
            try {
                 name = HELPER.getValue(TABLE_NAME, key+"", FAMILY_NAME, NAME);

                 city = HELPER.getValue(TABLE_NAME, key+"", FAMILY_NAME, CITY);

            } catch(Exception e) {
                e.printStackTrace();
            }

            loaded.put(key, new Organization(key, name, city));
        }

        return loaded;
    }

    @Override
    public Organization load(Long key) throws CacheLoaderException {

        log.info(">> Organization load(Long key) from store...");

        log.info("hbase load key : {}" , key);

        String name = null, city = null;
        try {
            name = HELPER.getValue(TABLE_NAME, key + "", FAMILY_NAME, NAME);
            city = HELPER.getValue(TABLE_NAME, key + "", FAMILY_NAME, CITY);

        } catch(Exception e) {
            e.printStackTrace();
        }
        return new Organization(key, name, city);
    }

    @Override
    public void write(Cache.Entry<? extends Long, ? extends Organization> entry) throws CacheWriterException {
    }

    @Override
    public void delete(Object key) throws CacheWriterException {
    }
}
