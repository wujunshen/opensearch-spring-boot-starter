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
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 12:18<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IndexApi {
  private final OpenSearchClient openSearchClient;

  /**
   * perform a refresh or flush operation
   *
   * @param response corresponding objects
   * @return whether the operation succeeds, true succeeds, false fails
   */
  private boolean doOperation(ShardStatistics response) {
    int failedShards = response.failures().size();
    if (failedShards == response.total().intValue()) {
      log.info("OS index refresh failure: {}", response.failures());
      return false;
    } else if (failedShards > 0) {
      log.info("OS index flush partial slice failure: {}", response.failures());
    }
    log.info("OS index refreshed successfully");
    return true;
  }

  /**
   * create index
   *
   * @param indexName index name
   * @return is the creation successful?
   * @throws IOException exception info
   */
  public boolean createIndex(String indexName) throws IOException {
    if (isExistedIndex(indexName)) {
      deleteIndex(indexName);
    }
    // The writing style is more concise than RestHighLevelClient
    CreateIndexResponse createIndexResponse =
        openSearchClient.indices().create(c -> c.index(indexName));

    log.info(
        "the index: {} creation is successful: {}", indexName, createIndexResponse.acknowledged());

    return Boolean.TRUE.equals(createIndexResponse.acknowledged());
  }

  /**
   * determine if index exists
   *
   * @param indexName index name
   * @return if index exists
   * @throws IOException exception info
   */
  public boolean isExistedIndex(String indexName) throws IOException {
    BooleanResponse booleanResponse = openSearchClient.indices().exists(e -> e.index(indexName));

    log.info("the index: {} exist: {}", indexName, booleanResponse.value());

    return booleanResponse.value();
  }

  /**
   * delete index
   *
   * @param indexName index name
   * @return whether the deletion is successful
   * @throws IOException exception info
   */
  public boolean deleteIndex(String indexName) throws IOException {
    DeleteIndexResponse deleteIndexResponse =
        openSearchClient.indices().delete(d -> d.index(indexName));

    log.info("the index: {} is deleted: {}", indexName, deleteIndexResponse.acknowledged());

    return deleteIndexResponse.acknowledged();
  }

  /**
   * create index - specify mapping
   *
   * @param indexName index name
   * @return is the creation successful
   * @throws IOException exception info
   */
  public boolean createIndexWithMapping(String indexName, TypeMapping typeMapping)
      throws IOException {
    if (isExistedIndex(indexName)) {
      deleteIndex(indexName);
    }

    CreateIndexResponse createIndexResponse =
        openSearchClient
            .indices()
            .create(
                createIndexRequest ->
                    createIndexRequest
                        .index(indexName)
                        // with lambda the following mapping will override the above mapping
                        .mappings(typeMapping));

    log.info("the index: {} is created: {}", indexName, createIndexResponse.acknowledged());

    return createIndexResponse.acknowledged();
  }

  /**
   * create index - creating mappings with json scripts
   *
   * @param indexName index name
   * @param mappingScript json scripts
   * @return is the creation successful
   * @throws IOException exception info
   */
  public boolean createIndexWithMapping(String indexName, String mappingScript) throws IOException {
    if (isExistedIndex(indexName)) {
      deleteIndex(indexName);
    }

    JsonpMapper mapper = openSearchClient._transport().jsonpMapper();
    JsonParser parser = Json.createParser(new StringReader(mappingScript));

    CreateIndexResponse createIndexResponse =
        openSearchClient
            .indices()
            .create(
                createIndexRequest ->
                    createIndexRequest
                        .index(indexName)
                        .mappings(TypeMapping._DESERIALIZER.deserialize(parser, mapper)));

    log.info("the index: {} is created: {}", indexName, createIndexResponse.acknowledged());

    return createIndexResponse.acknowledged();
  }

  /**
   * query index
   *
   * @param indexName index name
   * @return GetIndexResponse objects
   * @throws IOException exception info
   */
  public GetIndexResponse queryIndex(String indexName) throws IOException {
    GetIndexResponse getIndexResponse = openSearchClient.indices().get(i -> i.index(indexName));

    log.info("the index: {} infomation: {}", indexName, getIndexResponse);

    return getIndexResponse;
  }

  /**
   * query index related information
   *
   * @param indexName index name
   * @return GetIndexResponse objects
   * @throws IOException exception info
   */
  public GetIndexResponse queryIndexDetail(String indexName) throws IOException {
    GetIndexResponse getIndexResponse =
        openSearchClient.indices().get(getIndexRequest -> getIndexRequest.index(indexName));

    Map<String, Property> properties =
        Objects.requireNonNull(Objects.requireNonNull(getIndexResponse.get(indexName)).mappings())
            .properties();

    for (Map.Entry<String, Property> entry : properties.entrySet()) {
      log.info(
          "the index: {} detailed infomation: key: {}, property: {}",
          indexName,
          entry.getKey(),
          properties.get(entry.getKey())._kind());
    }

    return getIndexResponse;
  }

  /**
   * get all index information
   *
   * @return IndicesRecord list
   * @throws IOException exception info
   */
  public List<IndicesRecord> getAllIndices() throws IOException {
    List<IndicesRecord> indicesRecords = openSearchClient.cat().indices().valueBody();
    log.info("size is:{}", indicesRecords.size());

    return indicesRecords;
  }

  /**
   * get Mapping Information
   *
   * @param indexName index name
   * @return TypeMapping objects
   * @throws IOException exception info
   */
  public TypeMapping getMapping(String indexName) throws IOException {
    GetMappingResponse getMappingResponse = openSearchClient.indices().getMapping();

    TypeMapping typeMapping = getMappingResponse.result().get(indexName).mappings();

    log.info("typeMapping is:{}", typeMapping);

    return typeMapping;
  }

  /**
   * get all Mapping information
   *
   * @return Map<String, TypeMapping> objects，key is the index name, value is the TypeMapping object
   * @throws IOException exception info
   */
  public Map<String, TypeMapping> getAllMappings() throws IOException {
    Map<String, IndexMappingRecord> indexMappingRecordMap =
        openSearchClient.indices().getMapping().result();

    Map<String, TypeMapping> result = new HashMap<>(indexMappingRecordMap.size());

    for (Map.Entry<String, IndexMappingRecord> entry : indexMappingRecordMap.entrySet()) {
      String key = entry.getKey();
      TypeMapping typeMapping = indexMappingRecordMap.get(key).mappings();
      log.info("the index detailed infomation: key: {}, property: {}", key, typeMapping);

      result.put(key, typeMapping);
    }

    return result;
  }

  /**
   * index refresh
   *
   * @param indexName index name
   * @return whether refresh succeeds, true succeeds false fails
   * @throws IOException exception info
   */
  public boolean refresh(String indexName) throws IOException {
    RefreshResponse response =
        openSearchClient.indices().refresh(request -> request.index(indexName));

    return doOperation(response.shards());
  }

  /**
   * index flush
   *
   * @param indexName index name
   * @return whether flush succeeds, true succeeds false fails
   * @throws IOException exception info
   */
  public boolean flush(String indexName) throws IOException {
    FlushResponse response = openSearchClient.indices().flush(request -> request.index(indexName));

    return doOperation(response.shards());
  }
}
