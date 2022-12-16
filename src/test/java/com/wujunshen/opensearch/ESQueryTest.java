package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.product.Sku;
import com.wujunshen.entity.product.Spu;
import com.wujunshen.opensearch.api.DocumentApi;
import com.wujunshen.opensearch.api.IndexApi;
import com.wujunshen.opensearch.api.QueryApi;
import com.wujunshen.opensearch.config.OpenSearchConfigProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2020/2/7 5:33 下午 <br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(50)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class})//这里加启动类
public class ESQueryTest {

	@Autowired
	private OpenSearchConfigProperties openSearchConfigProperties;

	@Autowired
	private IndexApi indexApi;

	@Autowired
	private DocumentApi documentApi;

	@Autowired
	private QueryApi queryApi;

	private Spu spu1;
	private Spu spu2;
	private Spu spu3;

	private List<Spu> spuList = new ArrayList<>();

	private String indexName;

	@BeforeAll
	public void init() throws IOException {
		indexName = openSearchConfigProperties.getIndex();

		spu1 = new Spu();
		spu1.setId(1L);
		spu1.setProductCode("7b28c293-4d06-4893-aad7-e4b6ed72c260");
		spu1.setProductName("android手机");
		spu1.setBrandCode("H-001");
		spu1.setBrandName("华为Nexus");
		spu1.setCategoryCode("C-001");
		spu1.setCategoryName("手机");

		Sku sku1 = new Sku();
		sku1.setId(1L);
		sku1.setSkuCode("001");
		sku1.setSkuName("华为Nexus P6");
		sku1.setSkuPrice(4000);
		sku1.setColor("Red");

		Sku sku2 = new Sku();
		sku2.setId(2L);
		sku2.setSkuCode("002");
		sku2.setSkuName("华为 P8");
		sku2.setSkuPrice(3000);
		sku2.setColor("Blank");

		Sku sku3 = new Sku();
		sku3.setId(3L);
		sku3.setSkuCode("003");
		sku3.setSkuName("华为Nexus P6下一代");
		sku3.setSkuPrice(5000);
		sku3.setColor("White");

		spu1.getSkus().add(sku1);
		spu1.getSkus().add(sku2);
		spu1.getSkus().add(sku3);

		spu2 = new Spu();
		spu2.setId(2L);
		spu2.setProductCode("AVYmdpQ_cnzgjoSZ6ent");
		spu2.setProductName("运动服装");
		spu2.setBrandCode("YD-001");
		spu2.setBrandName("李宁");
		spu2.setCategoryCode("YDC-001");
		spu2.setCategoryName("服装");

		Sku sku21 = new Sku(21L, "YD001", "李宁衣服1", "Green", "2XL", 4000);
		Sku sku22 = new Sku(22L, "YD002", "李宁衣服2", "Green", "L", 3000);
		Sku sku23 = new Sku(23L, "YD003", "李宁衣服3", "Green", "M", 5000);

		spu2.getSkus().add(sku21);
		spu2.getSkus().add(sku22);
		spu2.getSkus().add(sku23);

		spu3 = new Spu();
		spu3.setId(3L);
		spu3.setProductCode("XYY1234567");
		spu3.setProductName("中华人民共和国");
		spu3.setBrandCode("YD-001");
		spu3.setBrandName("米老鼠");
		spu3.setCategoryCode("YDC-001");
		spu3.setCategoryName("服装");

		Sku sku31 = new Sku(31L, "LS001", "老鼠的帽子1", "Red", "L", 4000);
		Sku sku32 = new Sku(32L, "LS002", "老鼠的帽子2", "Yellow", "M", 3000);
		Sku sku33 = new Sku(33L, "LS003", "老鼠的帽子3", "Green", "2XL", 5000);

		spu3.getSkus().add(sku31);
		spu3.getSkus().add(sku32);
		spu3.getSkus().add(sku33);

		spuList.add(spu1);
		spuList.add(spu2);
		spuList.add(spu3);

		documentApi.batchAddDocument(indexName, spuList);

		indexApi.refresh(indexName);
	}

	@AfterAll
	public void clear() throws IOException {
		indexApi.deleteIndex(indexName);

		spu1 = null;
		spu2 = null;
		spu3 = null;

		spuList = null;

		indexName = null;
	}

	/**
	 * 空查询
	 */
	@Test
	public void getAllDocument() throws IOException {
		List<Spu> result = documentApi.getAllDocument(indexName, Spu.class);

		result.forEach(x -> log.info(String.valueOf(x)));

		assertThat(result, notNullValue());
		assertThat(result.size(), is(3));
		assertThat(result.get(0), equalTo(spu1));
		assertThat(result.get(1), equalTo(spu2));
		assertThat(result.get(2), equalTo(spu3));
	}

	@Test
	public void matchQuery() throws IOException {
		List<Spu> result = queryApi.matchQuery(indexName, "android手机", "productName", "id", 0, 1,
				true, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));

		result = queryApi.matchQuery(indexName, "android", "productName", "id", 0, 1, true, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));

		result = queryApi.matchQuery(indexName, "xxx", "productName", "id", 0, 1, true, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(0));
	}

	//
	@Test
	public void multiMatchQuery() throws IOException {
		List<String> fields = new ArrayList<>();
		fields.add("productName");

		List<Spu> result = queryApi.multiMatchQuery(indexName, "人民共和", fields, "id", 0, 1, true,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu3));
	}

	@Test
	public void termQuery() throws IOException {
		List<Spu> result = queryApi.termQuery(indexName, "android", "productName", "id", 0, 1, true,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));

		result = queryApi.termQuery(indexName, "android手机", "productName", "id", 0, 1, true,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(0));
	}

	@Test
	public void fuzzyQuery() throws IOException {
		List<Spu> result = queryApi.fuzzyQuery(indexName, "李", "brandName", "id", 0, 1, true,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu2));

		result = queryApi.fuzzyQuery(indexName, "李宁", "brandName", "id", 0, 1, true, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(0));
	}

	@Test
	public void matchPhrasePrefixQuery() throws IOException {
		List<Spu> result =
				queryApi.matchPhrasePrefixQuery(indexName, "人民共", "productName", "id", 0, 1, true,
						Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu3));

		result = queryApi.matchPhrasePrefixQuery(indexName, "鼠", "brandName", "id", 0, 1, true,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu3));
	}

	/**
	 * 根据ID做搜索
	 */
	@Test
	public void idsQuery() throws IOException {
		List<String> allDocumentIds = documentApi.getAllDocumentIds(indexName, Spu.class);

		List<Spu> result = queryApi.idsQuery(indexName, allDocumentIds, "id", 0, 10, false, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(3));
		assertThat(result.get(0), equalTo(spu1));
		assertThat(result.get(1), equalTo(spu2));
		assertThat(result.get(2), equalTo(spu3));
	}

	@Test
	public void rangeQuery() throws IOException {
		List<Spu> result =
				queryApi.rangeQuery(indexName, "android", "服装", "productName", "id", 0, 3, false,
						Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(3));
		assertThat(result.get(0), equalTo(spu1));
		assertThat(result.get(1), equalTo(spu2));
		assertThat(result.get(2), equalTo(spu3));
	}

	@Test
	public void wildcardQuery() throws IOException {
		// 避免*开始, 会检索大量内容造成效率缓慢,这里只是示例
		List<Spu> result = queryApi.wildcardQuery(indexName, "*", "productName", "id", 0, 3, false,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(3));
		assertThat(result.get(0), equalTo(spu1));
		assertThat(result.get(1), equalTo(spu2));
		assertThat(result.get(2), equalTo(spu3));

		result = queryApi.wildcardQuery(indexName, "an*d", "productName", "id", 0, 3, false, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void constantScoreQuery() throws IOException {
		List<Spu> result =
				queryApi.constantScoreQuery(indexName, "android", "productName", 0.2f, "id", 0, 3, false,
						Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void disMaxQuery() throws IOException {
		List<Query> queries = new ArrayList<>();

		Query qb1 = Query.of(q -> q.term(t -> t.field("productName").value(FieldValue.of("android"))));
		Query qb2 = Query.of(q -> q.term(t -> t.field("brandName").value(FieldValue.of("李宁"))));

		queries.add(qb1);
		queries.add(qb2);

		List<Spu> result = queryApi.disMaxQuery(indexName, queries, 1.3f, 0.7d, "id", 0, 3, false,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void queryStringQuery() throws IOException {
		List<Spu> result = queryApi.queryStringQuery(indexName, "+android", "id", 0, 3, false,
				Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void spanFirstQuery() throws IOException {
		List<Spu> result =
				queryApi.spanFirstQuery(indexName, "android", "productName", 30000, "id", 0, 3, false,
						Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void spanTermQuery() throws IOException {
		List<Spu> result = queryApi.spanTermQuery(indexName, "android", "productName", "id", 0, 3,
				false, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void boolQuery() throws IOException {
		List<Query> queries = new ArrayList<>();

		Query qb1 = Query.of(q -> q.term(t -> t.field("productName").value(FieldValue.of("android"))));

		queries.add(qb1);

		List<Spu> result = queryApi.boolQuery(indexName, queries, "id", 0, 3, false, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0), equalTo(spu1));
	}

	@Test
	public void matchAllQuery() throws IOException {
		List<Spu> result = queryApi.matchAllQuery(indexName, "id", 0, 3, false, Spu.class);
		result.forEach(spu -> log.info(String.valueOf(spu)));

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(3));
		assertThat(result.get(0), equalTo(spu1));
		assertThat(result.get(1), equalTo(spu2));
		assertThat(result.get(2), equalTo(spu3));
	}

	/**
	 * 对结果设置高亮显示
	 */
	@Test
	public void highLightResultSet() throws IOException {
//        Query query = QueryBuilders.wildcard((t -> t.field("productName").value("*")));
//
//        List<Map<String, List<String>>> result = queryApi.highLightQuery(
//                indexName, query, "productName", "<font color='red'>", "</font>", "id", 0, 3, false, Spu.class);
//        result.forEach(spu -> log.info(String.valueOf(spu)));
//
//        assertThat(result, notNullValue());
//        assertThat(result.size(), equalTo(3));
//
//        assertThat(result.get(0).get("productName").get(0), containsString("android"));
//        assertThat(result.get(1).get("productName").get(0), containsString("运"));
//        assertThat(result.get(2).get("productName").get(0), containsString("中"));
	}
}
