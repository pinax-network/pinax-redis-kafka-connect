package com.pinax.kafka.redis.connect.sink;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class RedisSinkConfigValidator implements ConfigDef.Validator {

      @SuppressWarnings("unchecked")
      public void ensureValid(String name, Object value) {

            if (name.equals(RedisSinkConfig.HOSTS)) {
                  List<String> hosts = (List<String>) value;
                  for (String host : hosts) {
                        String[] hostPort = host.split(":");
                        if (hostPort.length != 2) {
                              throw new ConfigException("Invalid value: " + value + ", expected format is host:port");
                        }
                        try {
                              Integer.parseInt(hostPort[1]);
                        } catch (NumberFormatException e) {
                              throw new ConfigException("Invalid value: " + value + ", expected format is host:port");
                        }
                  }
            }
      }
}
