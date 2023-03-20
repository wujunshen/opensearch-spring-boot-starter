package com.wujunshen.opensearch.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.HistogramBucket;
import org.opensearch.client.opensearch._types.query_dsl.ChildScoreMode;
import org.opensearch.client.opensearch._types.query_dsl.DisMaxQuery;
import org.opensearch.client.opensearch._types.query_dsl.MatchQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * the type query api.
 *
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022 /8/18 16:15<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QueryApi {

  private final OpenSearchClient openSearchClient;

  /**
   * get the source string in the Hit object
   *
   * @param response SearchResponse objects
   * @param <T> the generics of the document object
   * @return document object generic List
   */
  private <T> List<T> getSources(SearchResponse<T> response) {
    List<T> result = new ArrayList<>();
    for (Hit<T> hit : getHitList(response)) {
      result.add(hit.source());
    }
    return result;
  }

  /**
   * get the Map list of HighLight in the Hit object
   *
   * @param response SearchResponse objects
   * @param <T> the generics of the document object
   * @return Map list of the document object HighLight, key is the HighLight field name, value is
   *     the HighLight content
   */
  private <T> List<Map<String, List<String>>> getHighLights(SearchResponse<T> response) {
    return getHitList(response).stream().map(Hit::highlight).toList();
  }

  /**
   * get a list of Hit objects
   *
   * @param response SearchResponse objects
   * @param <T> the generics of the document object
   * @return document object generic List
   */
  private <T> List<Hit<T>> getHitList(SearchResponse<T> response) {
    log.info("consume times {} mill second", response.took());

    List<Hit<T>> hitList = response.hits().hits();

    if (CollectionUtils.isEmpty(hitList)) {
      return new ArrayList<>();
    }

    return hitList;
  }

  /**
   * specify id to retrieve data
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param id document id
   * @param clazz the document object class to search
   * @return GetResponse objects get response
   * @throws IOException exception info
   */
  public <T> GetResponse<T> searchById(String indexName, String id, Class<T> clazz)
      throws IOException {
    GetResponse<T> response = openSearchClient.get(g -> g.index(indexName).id(id), clazz);
    if (response.found()) {
      return response;
    } else {
      log.info("not found");
      return null;
    }
  }

  /**
   * aggregation operations
   *
   * @param indexName the index name
   * @param searchText search content
   * @param searchField search field
   * @param aggsField aggregate field
   * @param aggsKey aggregate key
   * @param intervalValue interval value
   * @return HistogramBucket list
   * @throws IOException exception info
   */
  public List<HistogramBucket> aggsByHistogram(
      String indexName,
      String searchText,
      String searchField,
      String aggsField,
      String aggsKey,
      Double intervalValue)
      throws IOException {
    Query query =
        MatchQuery.of(m -> m.field(searchField).query(FieldValue.of(searchText)))._toQuery();

    SearchResponse<Void> response =
        openSearchClient.search(
            b ->
                b.index(indexName)
                    .size(0)
                    .query(query)
                    .aggregations(
                        aggsKey, a -> a.histogram(h -> h.field(aggsField).interval(intervalValue))),
            Void.class);

    return response.aggregations().get(aggsKey).histogram().buckets().array();
  }

  /**
   * matchAllQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> matchAllQuery(
      String indexName,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.matchAll(t -> t))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * matchQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> matchQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.match(t -> t.field(searchField).query(FieldValue.of(searchText))))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * multiMatchQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchFields fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> multiMatchQuery(
      String indexName,
      String searchText,
      List<String> searchFields,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.multiMatch(t -> t.fields(searchFields).query(searchText)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * matchPhrasePrefixQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> matchPhrasePrefixQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.matchPhrasePrefix(t -> t.field(searchField).query(searchText)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * idsQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchTexts search content列表
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> idsQuery(
      String indexName,
      List<String> searchTexts,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.ids(t -> t.values(searchTexts)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * termQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> termQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.term(t -> t.field(searchField).value(FieldValue.of(searchText))))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * fuzzyQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> fuzzyQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.fuzzy(t -> t.field(searchField).value(FieldValue.of(searchText))))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * rangeQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param fromSearchText start search content
   * @param toSearchText content to complete the search
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> rangeQuery(
      String indexName,
      String fromSearchText,
      String toSearchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(
                        q ->
                            q.range(
                                t ->
                                    t.field(searchField)
                                        .from(JsonData.of(fromSearchText))
                                        .to(JsonData.of(toSearchText))))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * wildcardQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> wildcardQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.wildcard(t -> t.field(searchField).value(searchText)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * constantScoreQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param boost boost计分
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> constantScoreQuery(
      String indexName,
      String searchText,
      String searchField,
      float boost,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(
                        q ->
                            q.constantScore(
                                // wrapped query, higher than set score, no correlation calculated
                                p ->
                                    p.filter(
                                            e ->
                                                e.term(
                                                    t ->
                                                        t.field(searchField)
                                                            .value(FieldValue.of(searchText))))
                                        .boost(boost)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * disMaxQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param queries list of embedded query objects
   * @param boost boost计分
   * @param tieBreaker tieBreaker
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> disMaxQuery(
      String indexName,
      List<Query> queries,
      float boost,
      double tieBreaker,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    DisMaxQuery.Builder builder = QueryBuilders.disMax();
    builder.queries(queries);
    builder.boost(boost).tieBreaker(tieBreaker);

    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.disMax(builder.build()))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * queryStringQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> queryStringQuery(
      String indexName,
      String searchText,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.queryString(t -> t.query(searchText)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * spanFirstQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param end end
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> spanFirstQuery(
      String indexName,
      String searchText,
      String searchField,
      int end,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(
                        q ->
                            q.spanFirst(
                                t ->
                                    t.match(
                                            e ->
                                                e.spanTerm(
                                                    g -> g.field(searchField).value(searchText)))
                                        .end(end)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * spanTermQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param searchText search content
   * @param searchField fields to search
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> spanTermQuery(
      String indexName,
      String searchText,
      String searchField,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.spanTerm(g -> g.field(searchField).value(searchText)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);
    return getSources(response);
  }

  /**
   * boolQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param queries 内嵌的query对象列表
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> boolQuery(
      String indexName,
      List<Query> queries,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.bool(t -> t.must(queries)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * nestedQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param path path
   * @param query list of embedded query objects
   * @param childScoreMode ChildScoreMode枚举类值
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<T> nestedQuery(
      String indexName,
      String path,
      Query query,
      ChildScoreMode childScoreMode,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(q -> q.nested(t -> t.path(path).query(query).scoreMode(childScoreMode)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getSources(response);
  }

  /**
   * highLightQuery
   *
   * @param <T> the generics of the document object
   * @param indexName the index name
   * @param query embedded query objects
   * @param highlightField highlight a query field
   * @param preTags highlighted front section
   * @param postTags highlighted rear section
   * @param sortedField fields to be sorted
   * @param fromIndex paging data is taken from the page number
   * @param pageSize how many data are taken per page
   * @param isDesc whether descending order, true descending order, false ascending order
   * @param clazz the document object class to search
   * @return a collection of generic object lists
   * @throws IOException exception info
   */
  public <T> List<Map<String, List<String>>> highLightQuery(
      String indexName,
      Query query,
      String highlightField,
      String preTags,
      String postTags,
      String sortedField,
      int fromIndex,
      int pageSize,
      boolean isDesc,
      Class<T> clazz)
      throws IOException {
    SearchResponse<T> response =
        openSearchClient.search(
            s ->
                s.index(indexName)
                    .query(query)
                    .highlight(
                        h -> h.fields(highlightField, f -> f.preTags(preTags).postTags(postTags)))
                    // paging query, starting from the first fromIndex page query pageSize documents
                    .from(fromIndex)
                    .size(pageSize)
                    // sort by the field to be sorted in descending order
                    .sort(
                        f ->
                            f.field(
                                o ->
                                    o.field(sortedField)
                                        .order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
            clazz);

    return getHighLights(response);
  }
}
