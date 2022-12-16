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
 * The type Search api.
 *
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022 /8/18 16:15<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QueryApi {

	private final OpenSearchClient openSearchClient;

	/**
	 * 获取Hit对象中source字符串
	 *
	 * @param response SearchResponse对象
	 * @param <T>      文档对象泛型
	 * @return 文档对象泛型列表
	 */
	private <T> List<T> getSources(SearchResponse<T> response) {
		List<T> result = new ArrayList<>();
		for (Hit<T> hit : getHitList(response)) {
			result.add(hit.source());
		}
		return result;
	}

	/**
	 * 获取Hit对象中HighLight的Map列表
	 *
	 * @param response SearchResponse对象
	 * @param <T>      文档对象泛型
	 * @return 文档对象HighLight的Map列表, key为HighLight字段名，value为HighLight内容
	 */
	private <T> List<Map<String, List<String>>> getHighLights(SearchResponse<T> response) {
		return getHitList(response).stream().map(Hit::highlight).toList();
	}

	/**
	 * 获取Hit对象列表
	 *
	 * @param response SearchResponse对象
	 * @param <T>      文档对象泛型
	 * @return 文档对象Hit泛型列表
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
	 * 指定id检索数据
	 *
	 * @param <T>       文档对象泛型
	 * @param indexName 索引名
	 * @param id        文档id
	 * @param clazz     要搜索的文档对象class
	 * @return GetResponse对象 get response
	 * @throws IOException 异常信息
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
	 * 聚合操作
	 *
	 * @param indexName     索引名
	 * @param searchText    搜索内容
	 * @param searchField   搜索字段
	 * @param aggsField     聚合字段
	 * @param aggsKey       聚合key
	 * @param intervalValue 间隔值
	 * @return HistogramBucket列表 list
	 * @throws IOException 异常信息
	 */
	public List<HistogramBucket> aggsByHistogram(String indexName, String searchText,
			String searchField, String aggsField, String aggsKey, Double intervalValue)
			throws IOException {
		Query query = MatchQuery.of(m -> m.field(searchField).query(FieldValue.of(searchText)))
				._toQuery();

		SearchResponse<Void> response = openSearchClient.search(
				b -> b.index(indexName).size(0).query(query).aggregations(aggsKey,
						a -> a.histogram(h -> h.field(aggsField).interval(intervalValue))), Void.class);

		return response.aggregations().get(aggsKey).histogram().buckets().array();
	}

	/**
	 * matchAllQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> matchAllQuery(String indexName, String sortedField, int fromIndex,
			int pageSize, boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.matchAll(t -> t))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * matchQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> matchQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.match(t -> t.field(searchField).query(FieldValue.of(searchText))))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * multiMatchQuery方法
	 *
	 * @param <T>          文档对象泛型
	 * @param indexName    索引名
	 * @param searchText   搜索内容
	 * @param searchFields 要搜索的字段列表
	 * @param sortedField  要排序的字段
	 * @param fromIndex    分页数据从第几页开始取
	 * @param pageSize     每页取多少条数据
	 * @param isDesc       是否降序，true降序，false升序
	 * @param clazz        要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> multiMatchQuery(String indexName, String searchText, List<String> searchFields,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.multiMatch(t -> t.fields(searchFields).query(searchText)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * matchPhrasePrefixQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> matchPhrasePrefixQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.matchPhrasePrefix(t -> t.field(searchField).query(searchText)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * idsQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchTexts 搜索内容列表
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> idsQuery(String indexName, List<String> searchTexts, String sortedField,
			int fromIndex, int pageSize, boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.ids(t -> t.values(searchTexts)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * termQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> termQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.term(t -> t.field(searchField).value(FieldValue.of(searchText))))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * fuzzyQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> fuzzyQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.fuzzy(t -> t.field(searchField).value(FieldValue.of(searchText))))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * rangeQuery方法
	 *
	 * @param <T>            文档对象泛型
	 * @param indexName      索引名
	 * @param fromSearchText 开始搜索的内容
	 * @param toSearchText   完成搜索的内容
	 * @param searchField    要搜索的字段
	 * @param sortedField    要排序的字段
	 * @param fromIndex      分页数据从第几页开始取
	 * @param pageSize       每页取多少条数据
	 * @param isDesc         是否降序，true降序，false升序
	 * @param clazz          要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> rangeQuery(String indexName, String fromSearchText, String toSearchText,
			String searchField, String sortedField, int fromIndex, int pageSize, boolean isDesc,
			Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName).query(q -> q.range(
								t -> t.field(searchField).from(JsonData.of(fromSearchText)).to(JsonData.of(toSearchText))))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * wildcardQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> wildcardQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.wildcard(t -> t.field(searchField).value(searchText)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * constantScoreQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param boost       boost计分
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> constantScoreQuery(String indexName, String searchText, String searchField,
			float boost, String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.constantScore(
								// 包裹查询, 高于设定分数, 不计算相关性
								p -> p.filter(e -> e.term(t -> t.field(searchField).value(FieldValue.of(searchText))))
										.boost(boost)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * disMaxQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param queries     内嵌的query对象列表
	 * @param boost       boost计分
	 * @param tieBreaker  tieBreaker
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> disMaxQuery(String indexName, List<Query> queries, float boost,
			double tieBreaker, String sortedField, int fromIndex, int pageSize, boolean isDesc,
			Class<T> clazz) throws IOException {
		DisMaxQuery.Builder builder = QueryBuilders.disMax();
		builder.queries(queries);
		builder.boost(boost).tieBreaker(tieBreaker);

		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.disMax(builder.build()))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * queryStringQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> queryStringQuery(String indexName, String searchText, String sortedField,
			int fromIndex, int pageSize, boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.queryString(t -> t.query(searchText)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * spanFirstQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param end         end
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> spanFirstQuery(String indexName, String searchText, String searchField,
			int end, String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName).query(
								q -> q.spanFirst(
										t -> t.match(e -> e.spanTerm(g -> g.field(searchField).value(searchText))).end(end)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * spanTermQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param searchText  搜索内容
	 * @param searchField 要搜索的字段
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> spanTermQuery(String indexName, String searchText, String searchField,
			String sortedField, int fromIndex, int pageSize, boolean isDesc, Class<T> clazz)
			throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.spanTerm(g -> g.field(searchField).value(searchText)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);
		return getSources(response);
	}

	/**
	 * boolQuery方法
	 *
	 * @param <T>         文档对象泛型
	 * @param indexName   索引名
	 * @param queries     内嵌的query对象列表
	 * @param sortedField 要排序的字段
	 * @param fromIndex   分页数据从第几页开始取
	 * @param pageSize    每页取多少条数据
	 * @param isDesc      是否降序，true降序，false升序
	 * @param clazz       要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> boolQuery(String indexName, List<Query> queries, String sortedField,
			int fromIndex, int pageSize, boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(
				s -> s.index(indexName).query(q -> q.bool(t -> t.must(queries)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(f -> f.field(
								o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))), clazz);

		return getSources(response);
	}

	/**
	 * nestedQuery方法
	 *
	 * @param <T>            文档对象泛型
	 * @param indexName      索引名
	 * @param path           path
	 * @param query          内嵌的query对象
	 * @param childScoreMode ChildScoreMode枚举类值
	 * @param sortedField    要排序的字段
	 * @param fromIndex      分页数据从第几页开始取
	 * @param pageSize       每页取多少条数据
	 * @param isDesc         是否降序，true降序，false升序
	 * @param clazz          要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<T> nestedQuery(String indexName, String path, Query query,
			ChildScoreMode childScoreMode, String sortedField, int fromIndex, int pageSize,
			boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName)
						.query(q -> q.nested(t -> t.path(path).query(query).scoreMode(childScoreMode)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getSources(response);
	}

	/**
	 * highLightQuery方法
	 *
	 * @param <T>            文档对象泛型
	 * @param indexName      索引名
	 * @param query          内嵌的query对象
	 * @param highlightField 高亮查询字段
	 * @param preTags        高亮前置部分
	 * @param postTags       高亮后置部分
	 * @param sortedField    要排序的字段
	 * @param fromIndex      分页数据从第几页开始取
	 * @param pageSize       每页取多少条数据
	 * @param isDesc         是否降序，true降序，false升序
	 * @param clazz          要搜索的文档对象class
	 * @return 泛型对象列表集合 list
	 * @throws IOException 异常信息
	 */
	public <T> List<Map<String, List<String>>> highLightQuery(String indexName, Query query,
			String highlightField, String preTags, String postTags, String sortedField, int fromIndex,
			int pageSize, boolean isDesc, Class<T> clazz) throws IOException {
		SearchResponse<T> response = openSearchClient.search(s -> s.index(indexName).query(query)
						.highlight(h -> h.fields(highlightField, f -> f.preTags(preTags).postTags(postTags)))
						// 分页查询，从第fromIndex页开始查询pageSize个document
						.from(fromIndex).size(pageSize)
						// 按要排序字段进行降序排序
						.sort(
								f -> f.field(o -> o.field(sortedField).order(isDesc ? SortOrder.Desc : SortOrder.Asc))),
				clazz);

		return getHighLights(response);
	}
}
