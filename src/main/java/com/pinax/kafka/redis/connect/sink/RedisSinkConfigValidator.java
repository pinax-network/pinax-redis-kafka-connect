package com.pinax.kafka.redis.connect.sink;

import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

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

            if (name.equals(RedisSinkConfig.FROM)) {
                  String from = (String) value;
                  try {
                        InternetAddress address = new InternetAddress(from);
                        address.validate();
                  } catch (AddressException e) {
                        throw new ConfigException("Invalid value: " + value + ", must be a valid email address");
                  }
            }
      }
}
