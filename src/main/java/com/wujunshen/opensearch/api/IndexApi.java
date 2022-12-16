package com.wujunshen.opensearch.api;

import jakarta.json.Json;
import jakarta.json.stream.JsonParser;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ShardStatistics;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.FlushResponse;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.RefreshResponse;
import org.opensearch.client.opensearch.indices.get_mapping.IndexMappingRecord;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.springframework.stereotype.Component;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br> {@code @date} 2022/8/18
 * 12:18<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IndexApi {
	private final OpenSearchClient openSearchClient;

	/**
	 * 执行refresh或flush操作
	 *
	 * @param response 相应对象
	 * @return 操作是否成功，true成功 false失败
	 */
	private boolean doOperation(ShardStatistics response) {
		int failedShards = response.failures().size();
		if (failedShards == response.total().intValue()) {
			log.info("ES索引刷新失败 {}", response.failures());
			return false;
		} else if (failedShards > 0) {
			log.info("ES索引刷新部分分片失败 {}", response.failures());
		}
		log.info("ES索引刷新成功");
		return true;
	}

	/**
	 * 创建索引
	 *
	 * @param indexName 索引名
	 * @return 是否创建成功
	 * @throws IOException 异常信息
	 */
	public boolean createIndex(String indexName) throws IOException {
		if (isExistedIndex(indexName)) {
			deleteIndex(indexName);
		}
		// 写法比RestHighLevelClient更加简洁
		CreateIndexResponse createIndexResponse = openSearchClient.indices()
				.create(c -> c.index(indexName));

		log.info("{} 索引创建是否成功: {}", indexName, createIndexResponse.acknowledged());

		return Boolean.TRUE.equals(createIndexResponse.acknowledged());
	}

	/**
	 * 判断index是否存在
	 *
	 * @param indexName 索引名
	 * @return 是否存在
	 * @throws IOException 异常信息
	 */
	public boolean isExistedIndex(String indexName) throws IOException {
		BooleanResponse booleanResponse = openSearchClient.indices().exists(e -> e.index(indexName));

		log.info("{} 索引是否存在: {}", indexName, booleanResponse.value());

		return booleanResponse.value();
	}

	/**
	 * 删除index
	 *
	 * @param indexName 索引名
	 * @return 是否删除成功
	 * @throws IOException 异常信息
	 */
	public boolean deleteIndex(String indexName) throws IOException {
		DeleteIndexResponse deleteIndexResponse = openSearchClient.indices()
				.delete(d -> d.index(indexName));

		log.info("{} 索引是否被删除: {}", indexName, deleteIndexResponse.acknowledged());

		return deleteIndexResponse.acknowledged();
	}

	/**
	 * 创建索引 - 指定mapping
	 *
	 * @param indexName 索引名
	 * @return 是否创建成功
	 * @throws IOException 异常信息
	 */
	public boolean createIndexWithMapping(String indexName, TypeMapping typeMapping)
			throws IOException {
		if (isExistedIndex(indexName)) {
			deleteIndex(indexName);
		}

		CreateIndexResponse createIndexResponse = openSearchClient.indices()
				.create(createIndexRequest -> createIndexRequest.index(indexName)
						// 用 lambda 的方式 下面的 mapping 会覆盖上面的 mapping
						.mappings(typeMapping));

		log.info("{} 索引创建是否成功: {}", indexName, createIndexResponse.acknowledged());

		return createIndexResponse.acknowledged();
	}

	/**
	 * 创建索引 - 用json脚本创建mapping
	 *
	 * @param indexName     索引名
	 * @param mappingScript mapping的json脚本
	 * @return 是否创建成功
	 * @throws IOException 异常信息
	 */
	public boolean createIndexWithMapping(String indexName, String mappingScript) throws IOException {
		if (isExistedIndex(indexName)) {
			deleteIndex(indexName);
		}

		JsonpMapper mapper = openSearchClient._transport().jsonpMapper();
		JsonParser parser = Json.createParser(new StringReader(mappingScript));

		CreateIndexResponse createIndexResponse = openSearchClient.indices().create(
				createIndexRequest -> createIndexRequest.index(indexName)
						.mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper)));

		log.info("{} 索引创建是否成功: {}", indexName, createIndexResponse.acknowledged());

		return createIndexResponse.acknowledged();
	}

	/**
	 * 查询index
	 *
	 * @param indexName 索引名
	 * @return GetIndexResponse对象
	 * @throws IOException 异常信息
	 */
	public GetIndexResponse queryIndex(String indexName) throws IOException {
		GetIndexResponse getIndexResponse = openSearchClient.indices().get(i -> i.index(indexName));

		log.info("{} 索引信息: {}", indexName, getIndexResponse);

		return getIndexResponse;
	}

	/**
	 * 查询index相关信息
	 *
	 * @param indexName 索引名
	 * @return GetIndexResponse对象
	 * @throws IOException 异常信息
	 */
	public GetIndexResponse queryIndexDetail(String indexName) throws IOException {
		GetIndexResponse getIndexResponse = openSearchClient.indices()
				.get(getIndexRequest -> getIndexRequest.index(indexName));

		Map<String, Property> properties = Objects.requireNonNull(
				Objects.requireNonNull(getIndexResponse.get(indexName)).mappings()).properties();

		for (Map.Entry<String, Property> entry : properties.entrySet()) {
			log.info("{} 索引的详细信息为: key: {}, property: {}", indexName, entry.getKey(),
					properties.get(entry.getKey())._kind());
		}

		return getIndexResponse;
	}

	/**
	 * 获取所有索引信息
	 *
	 * @return IndicesRecord列表
	 * @throws IOException 异常信息
	 */
	public List<IndicesRecord> getAllIndices() throws IOException {
		List<IndicesRecord> indicesRecords = openSearchClient.cat().indices().valueBody();
		log.info("size is:{}", indicesRecords.size());

		return indicesRecords;
	}

	/**
	 * 获取Mapping信息
	 *
	 * @param indexName 索引名
	 * @return TypeMapping对象
	 * @throws IOException 异常信息
	 */
	public TypeMapping getMapping(String indexName) throws IOException {
		GetMappingResponse getMappingResponse = openSearchClient.indices().getMapping();

		TypeMapping typeMapping = getMappingResponse.result().get(indexName).mappings();

		log.info("typeMapping is:{}", typeMapping);

		return typeMapping;
	}

	/**
	 * 获取所有Mapping信息
	 *
	 * @return Map<String, TypeMapping>对象，key就是索引名，value是TypeMapping对象
	 * @throws IOException 异常信息
	 */
	public Map<String, TypeMapping> getAllMappings() throws IOException {
		Map<String, IndexMappingRecord> indexMappingRecordMap = openSearchClient.indices().getMapping()
				.result();

		Map<String, TypeMapping> result = new HashMap<>(indexMappingRecordMap.size());

		for (Map.Entry<String, IndexMappingRecord> entry : indexMappingRecordMap.entrySet()) {
			String key = entry.getKey();
			TypeMapping typeMapping = indexMappingRecordMap.get(key).mappings();
			log.info("索引的详细信息为: key: {}, property: {}", key, typeMapping);

			result.put(key, typeMapping);
		}

		return result;
	}

	/**
	 * 索引refresh
	 *
	 * @param indexName 索引名
	 * @return refresh是否成功，true成功 false失败
	 * @throws IOException 异常信息
	 */
	public boolean refresh(String indexName) throws IOException {
		RefreshResponse response = openSearchClient.indices()
				.refresh(request -> request.index(indexName));

		return doOperation(response.shards());
	}

	/**
	 * 索引flush
	 *
	 * @param indexName 索引名
	 * @return flush是否成功，true成功 false失败
	 * @throws IOException 异常信息
	 */
	public boolean flush(String indexName) throws IOException {
		FlushResponse response = openSearchClient.indices().flush(request -> request.index(indexName));

		return doOperation(response.shards());
	}
}
