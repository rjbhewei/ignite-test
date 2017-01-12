package com.shuyun.demo.ignite.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.annotation.Nullable;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/4  14:28
 * @desc
 */
@Slf4j
public class PersonStore extends CacheStoreAdapter<Long, Person>{

    private DriverManagerDataSource dataSource = SpringDateSource.dataSource();

    @Override
    public void loadCache(IgniteBiInClosure<Long, Person> clo, @Nullable Object... objects) throws CacheLoaderException {
        log.info(">> Loading cache from store...");
        try (Connection conn = dataSource.getConnection()) {
            try(PreparedStatement st = conn.prepareStatement("select * from person")) {
                try(ResultSet rs = st.executeQuery()) {
                    while(rs.next()) {
                        Person person = new Person(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getInt(4));
                        clo.apply(person.getId(), person);
                    }
                }
            }
        } catch(SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    @Override
    public Person load(Long key) throws CacheLoaderException {
        log.info(">> Loading person from store...");
        try (Connection conn = dataSource.getConnection()) {
            try(PreparedStatement st = conn.prepareStatement("select * from person where id = ?")) {
                st.setString(1, key.toString());
                ResultSet rs = st.executeQuery();
                return rs.next() ? new Person(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getInt(4)) : null;
            }
        } catch(SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }


    @Override
    public void write(Cache.Entry<? extends Long, ? extends Person> entry) throws CacheWriterException {
    }
    @Override
    public void delete(Object key) throws CacheWriterException {
    }
}
