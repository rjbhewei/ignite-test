package com.shuyun.demo.test.base;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/8  18:39
 * @desc
 */
@Slf4j
public class Base1 {

    public static void main(String[] args) throws IgniteCheckedException {

        URL logUrl = ClassLoader.getSystemClassLoader().getResource("log4j2.xml");

        if(logUrl == null) {
            System.exit(0);
        }

        System.setProperty("log4j.configuration", logUrl.getPath());

        IgniteLogger igniteLogger = new Log4J2Logger(logUrl.getPath());

        IgniteConfiguration configuration = new IgniteConfiguration();

        configuration.setGridLogger(igniteLogger);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Lists.newArrayList("172.18.53.124","172.18.2.40"));

        discoverySpi.setIpFinder(ipFinder);

        configuration.setDiscoverySpi(discoverySpi);

        configuration.setPeerClassLoadingEnabled(true);

        configuration.setClientMode(true);

        try (Ignite ignite = Ignition.start(configuration)) {

            Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

            // Iterate through all the words in the sentence and create Callable jobs.
            for (final String word : "Count characters using callable".split(" ")) {
                calls.add((IgniteCallable<Integer>) word::length);
            }

            // Execute collection of Callables on the grid.
            Collection<Integer> res = ignite.compute().call(calls);

            int sum = 0;

            // Add up individual word lengths received from remote nodes.
            for (int len : res)
                sum += len;

            log.info(">>> Total number of characters in the phrase is {} .",sum);
        }
    }
}
