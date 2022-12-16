package com.wujunshen.opensearch.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/10/23 20:47<br>
 */
@Data
@ConfigurationProperties(prefix = "ssl.trust.store")
public class SslConfigProperties {

	/**
	 * trust store文件所在路径
	 */
	private String path;

	/**
	 * trust store密码
	 */
	private String password;
}
