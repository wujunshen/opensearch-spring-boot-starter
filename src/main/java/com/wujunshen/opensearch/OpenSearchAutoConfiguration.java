package com.wujunshen.opensearch;

import com.wujunshen.opensearch.config.OpenSearchConfigProperties;
import com.wujunshen.opensearch.config.SslConfigProperties;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/12/14 16:39<br>
 */
@Configuration
@EnableConfigurationProperties({OpenSearchConfigProperties.class, SslConfigProperties.class})
public class OpenSearchAutoConfiguration {

	@ConditionalOnMissingBean
	@Bean
	public CredentialsProvider credentialsProvider(
			OpenSearchConfigProperties openSearchConfigProperties) {
		BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(openSearchConfigProperties.getUserName(),
						openSearchConfigProperties.getPassword()));
		return credentialsProvider;
	}

	@ConditionalOnMissingBean
	@Bean
	public RestClient restClient(OpenSearchConfigProperties openSearchConfigProperties,
			SslConfigProperties sslConfigProperties, CredentialsProvider credentialsProvider) {
		// 拆分地址
		List<HttpHost> httpHostList = new ArrayList<>();
		String[] hostArray = openSearchConfigProperties.getAddress().split(",");
		for (String element : hostArray) {
			String host = element.split(":")[0];
			String port = element.split(":")[1];
			httpHostList.add(
					new HttpHost(host, Integer.parseInt(port), openSearchConfigProperties.getSchema()));
		}

		// 转换成 HttpHost 数组
		HttpHost[] httpHostArray = httpHostList.toArray(new HttpHost[]{});
		// 构建连接对象
		RestClientBuilder builder = RestClient.builder(httpHostArray);

		// 异步连接延时配置
		builder.setRequestConfigCallback(requestConfigBuilder -> {
			requestConfigBuilder.setConnectTimeout(openSearchConfigProperties.getConnectTimeout());
			requestConfigBuilder.setSocketTimeout(openSearchConfigProperties.getSocketTimeout());
			requestConfigBuilder.setConnectionRequestTimeout(
					openSearchConfigProperties.getConnectionRequestTimeout());
			return requestConfigBuilder;
		});

		// 异步连接数配置
		builder.setHttpClientConfigCallback(httpClientBuilder -> {
			httpClientBuilder.setMaxConnTotal(openSearchConfigProperties.getMaxConnectNum());
			httpClientBuilder.setMaxConnPerRoute(openSearchConfigProperties.getMaxConnectPerRoute());
			return httpClientBuilder;
		});

		if ("https".equals(openSearchConfigProperties.getSchema())) {
			System.setProperty("javax.net.ssl.trustStore", sslConfigProperties.getPath());
			System.setProperty("javax.net.ssl.trustStorePassword", sslConfigProperties.getPassword());

			builder.setHttpClientConfigCallback(httpClientBuilder -> {
				// 不验证hostname
				httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);

				//设置用户名和密码访问
				httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

				return httpClientBuilder;
			});
		}

		return builder.build();
	}

	@ConditionalOnMissingBean
	@Bean
	public OpenSearchTransport openSearchTransport(RestClient restClient) {
		return new RestClientTransport(restClient, new JacksonJsonpMapper());
	}

	@ConditionalOnMissingBean
	@Bean
	public OpenSearchClient openSearchClient(OpenSearchTransport transport) {
		return new OpenSearchClient(transport);
	}
}
