package com.wujunshen.opensearch.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * es配置类
 *
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 12:05<br>
 */
@Data
@ConfigurationProperties(prefix = "opensearch")
public class OpenSearchConfigProperties {

	/**
	 * 协议
	 */
	private String schema;

	/**
	 * 集群地址，如果有多个用“,”隔开
	 */
	private String address;

	/**
	 * 连接超时时间
	 */
	private int connectTimeout;

	/**
	 * Socket 连接超时时间
	 */
	private int socketTimeout;

	/**
	 * 获取连接的超时时间
	 */
	private int connectionRequestTimeout;

	/**
	 * 最大连接数
	 */
	private int maxConnectNum;

	/**
	 * 最大路由连接数
	 */
	private int maxConnectPerRoute;

	/**
	 * 连接ES的用户名
	 */
	private String userName = "admin";

	/**
	 * 数据查询的索引
	 */
	private String index;

	/**
	 * 密码
	 */
	private String password = "admin";
}
