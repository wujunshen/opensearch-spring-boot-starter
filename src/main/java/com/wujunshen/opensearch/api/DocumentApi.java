package com.wujunshen.opensearch.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/18 12:46<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentApi {

  private final OpenSearchClient openSearchClient;

  /**
   * write the single document
   *
   * @param indexName index name
   * @param o document object
   * @param <T> the generics of the document object
   * @return IndexResponse object
   * @throws IOException exception info
   */
  public <T> IndexResponse addDocument(String indexName, T o) throws IOException {
    IndexResponse indexResponse =
        openSearchClient.index(indexRequest -> indexRequest.index(indexName).document(o));

    log.info("response: {}", indexResponse);

    return indexResponse;
  }

  /**
   * write the single document
   *
   * @param indexName index name
   * @param id document id
   * @param o document object
   * @param <T> the generics of the document object
   * @return IndexResponse object
   * @throws IOException exception info
   */
  public <T> IndexResponse addDocument(String indexName, String id, T o) throws IOException {
    IndexResponse indexResponse =
        openSearchClient.index(indexRequest -> indexRequest.index(indexName).id(id).document(o));

    log.info("response: {}", indexResponse);

    return indexResponse;
  }

  /**
   * update document information
   *
   * @param indexName index name
   * @param o document object
   * @param id the document object id to be updated
   * @param clazz the document object class to update
   * @param <T> the generics of the document object
   * @return UpdateResponse object
   * @throws IOException exception info
   */
  public <T> UpdateResponse<T> updateDocument(String indexName, T o, String id, Class<T> clazz)
      throws IOException {
    UpdateResponse<T> updateResponse =
        openSearchClient.update(
            updateRequest -> updateRequest.index(indexName).id(id).doc(o), clazz);

    log.info("response: {}", updateResponse);

    return updateResponse;
  }

  /**
   * query document information
   *
   * @param indexName index name
   * @param id the document object id to be queried
   * @param clazz the document object class to be queried
   * @param <T> the generics of the document object
   * @return GetResponse object
   * @throws IOException exception info
   */
  public <T> GetResponse<T> getDocument(String indexName, String id, Class<T> clazz)
      throws IOException {
    GetResponse<T> getResponse =
        openSearchClient.get(getRequest -> getRequest.index(indexName).id(id), clazz);

    log.info("document source: {}, response: {}", getResponse.source(), getResponse);

    return getResponse;
  }

  /**
   * get the information about all documents under the index
   *
   * @param indexName index name
   * @param clazz the document object class to be queried
   * @param <T> the generics of the document object
   * @return the generics of the document object list
   * @throws IOException exception info
   */
  public <T> List<T> getAllDocument(String indexName, Class<T> clazz) throws IOException {
    SearchResponse<T> searchResponse = openSearchClient.search(a -> a.index(indexName), clazz);

    List<Hit<T>> hitList = getHitList(searchResponse);

    return hitList.stream().map(Hit::source).toList();
  }

  /**
   * get all document ids under index
   *
   * @param indexName index name
   * @param clazz the document object class to be queried
   * @param <T> the generics of the document object
   * @return document object id list
   * @throws IOException exception info
   */
  public <T> List<String> getAllDocumentIds(String indexName, Class<T> clazz) throws IOException {
    SearchResponse<T> searchResponse = openSearchClient.search(a -> a.index(indexName), clazz);

    List<Hit<T>> hitList = getHitList(searchResponse);

    return hitList.stream().map(Hit::id).toList();
  }

  /**
   * delete document information
   *
   * @param indexName index name
   * @param id the document object id to be deleted
   * @return DeleteResponse object
   * @throws IOException exception info
   */
  public DeleteResponse deleteDocument(String indexName, String id) throws IOException {
    DeleteResponse deleteResponse =
        openSearchClient.delete(deleteRequest -> deleteRequest.index(indexName).id(id));

    log.info("response: {}, result:{}", deleteResponse, deleteResponse.result());

    return deleteResponse;
  }

  /**
   * delete all document information
   *
   * @param indexName index name
   * @param clazz the generics of the document object class
   * @param <T> the generics of the document object
   * @return is the deletion successful?
   * @throws IOException exception info
   */
  public <T> boolean deleteAllDocument(String indexName, Class<T> clazz) throws IOException {
    return batchDeleteDocument(indexName, getAllDocumentIds(indexName, clazz));
  }

  /**
   * batch document insertion
   *
   * @param indexName index name
   * @param list batch inserted document object list
   * @param <T> the generics of the document object
   * @return whether the batch insertion is successful?
   * @throws IOException exception info
   */
  public <T> boolean batchAddDocument(String indexName, List<T> list) throws IOException {
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (T element : list) {
      br.operations(op -> op.index(idx -> idx.index(indexName).document(element)));
    }

    BulkResponse bulkResponse = openSearchClient.bulk(br.build());

    if (bulkResponse.errors()) {
      log.error("Bulk had errors");
      for (BulkResponseItem item : bulkResponse.items()) {
        if (item.error() != null) {
          log.error("{}", item.error().reason());
        }
      }
      return false;
    } else {
      log.info("Bulk write success!");
      return true;
    }
  }

  /**
   * batch delete documents
   *
   * @param indexName index name
   * @param ids list of document ids for batch deletion
   * @param <T> the generics of the document object
   * @return whether the batch deletion is successful?
   * @throws IOException exception info
   */
  public <T> boolean batchDeleteDocument(String indexName, List<String> ids) throws IOException {
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (String id : ids) {
      br.operations(op -> op.delete(idx -> idx.index(indexName).id(id)));
    }

    BulkResponse bulkResponse = openSearchClient.bulk(br.build());

    if (bulkResponse.errors()) {
      log.error("Bulk had errors");
      for (BulkResponseItem item : bulkResponse.items()) {
        if (item.error() != null) {
          log.error("{}", item.error().reason());
        }
      }
      return false;
    } else {
      log.info("Bulk delete success!");
      return true;
    }
  }

  /**
   * get a list of Hit objects
   *
   * @param response SearchResponse object
   * @param <T> the generics of the document object
   * @return the generics of the document object list
   */
  private <T> List<Hit<T>> getHitList(SearchResponse<T> response) {
    log.info("consume times {} mill second", response.took());

    List<Hit<T>> hitList = response.hits().hits();

    if (CollectionUtils.isEmpty(hitList)) {
      return new ArrayList<>();
    }

    return hitList;
  }
}
