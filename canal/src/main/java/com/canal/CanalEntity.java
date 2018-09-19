package com.canal;

import java.io.IOException;
import java.util.Properties;

public class CanalEntity {

    public final String canalHostIp;
    public final Integer canalHostPort;
    public final String canalDesignation;
    public final String mysqlUserName;
    public final String mysqlPassword;
    public final Integer batchSize;
    volatile private static CanalEntity instance;

    private CanalEntity(String canalHostIp, Integer canalHostPort, String canalDesignation, String mysqlUserName, String mysqlPassword, Integer batchSize) {
        this.canalHostIp = canalHostIp;
        this.canalHostPort = canalHostPort;
        this.canalDesignation = canalDesignation;
        this.mysqlUserName = mysqlUserName;
        this.mysqlPassword = mysqlPassword;
        this.batchSize = batchSize;
    }


    private static CanalEntity instance() {
        Properties props = loadProperties();
        String hostName = props.getProperty("canal.host.ip", "127.0.0.1");
        String port = props.getProperty("canal.host.port", "1111");
        String designation = props.getProperty("canal.host.designation", "example");
        String userName = props.getProperty("canal.mysql.username", "canal");
        String password = props.getProperty("canal.mysql.password", "canal");
        String batchSize = props.getProperty("canal.batch.size", "1024");
        props.clear();
        return new CanalEntity(hostName, Integer.valueOf(port), designation, userName, password, Integer.valueOf(batchSize));
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try {
            props.load(CanalEntity.class.getClassLoader().getResourceAsStream("canal-client.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static CanalEntity getInstance() {
        if (instance == null) {
            synchronized (CanalEntity.class) {
                if (instance == null) instance = instance();
            }
        }
        return instance;
    }

}
