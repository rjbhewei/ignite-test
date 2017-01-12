package com.shuyun.demo.test.ipFinder;

import com.google.common.collect.Lists;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * @author hewei
 * @version 5.0
 * @date 2017/1/8  17:24
 * @desc
 */
public class IpFinder2 {

    /**
     * 静态ip
     * @param args
     */
    public static void main(String[] args) {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Lists.newArrayList("172.18.2.40"));
        spi.setIpFinder(ipFinder);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDiscoverySpi(spi);
        Ignition.start(cfg);
    }
}
