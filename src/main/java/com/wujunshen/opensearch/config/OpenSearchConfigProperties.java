package com.wujunshen.opensearch.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * os configuration class
 *
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 12:05<br>
 */
@Data
@ConfigurationProperties(prefix = "opensearch")
public class OpenSearchConfigProperties {

  /** protocols https or http */
  private String schema;

  /** cluster address, separated by "," if there is more than one */
  private String address;

  /** connection timeout time */
  private int connectTimeout;

  /** Socket connection timeout time */
  private int socketTimeout;

  /** get connection's timeout time */
  private int connectionRequestTimeout;

  /** the max number of connections */
  private int maxConnectNum;

  /** the max number of routing connections */
  private int maxConnectPerRoute;

  /** username for connecting to os */
  private String userName = "admin";

  /** the index of data queries */
  private String index;

  /** password for connecting to os */
  private String password = "admin";
}
