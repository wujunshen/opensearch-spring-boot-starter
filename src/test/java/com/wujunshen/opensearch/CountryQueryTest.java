package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.area.City;
import com.wujunshen.entity.area.Country;
import com.wujunshen.entity.area.District;
import com.wujunshen.entity.area.Province;
import com.wujunshen.entity.area.Region;
import com.wujunshen.opensearch.api.DocumentApi;
import com.wujunshen.opensearch.api.IndexApi;
import com.wujunshen.opensearch.api.QueryApi;
import com.wujunshen.opensearch.config.OpenSearchConfigProperties;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
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
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.Property.Builder;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch._types.query_dsl.ChildScoreMode;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.util.ObjectBuilder;
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
@Order(400)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class})//这里加启动类
public class CountryQueryTest {

	private final ObjectMapper mapper = new ObjectMapper();

	@Autowired
	private OpenSearchConfigProperties openSearchConfigProperties;
	@Autowired
	private IndexApi indexApi;
	@Autowired
	private DocumentApi documentApi;
	@Autowired
	private QueryApi queryApi;

	private String indexName;

	@BeforeAll
	public void init() throws IOException {
		indexName = openSearchConfigProperties.getIndex();

		if (indexApi.isExistedIndex(indexName)) {
			indexApi.deleteIndex(indexName);
		}

		Function<Builder, ObjectBuilder<Property>> districtFn = fn -> fn.nested(
				district -> district.properties("id",
								id -> id.long_(longProperty -> longProperty.index(true)))
						.properties("name",
								name -> name.keyword(keyWordProperty -> keyWordProperty.index(true)))
						.properties("code", code -> code.integer(intProperty -> intProperty.index(true))));

		Function<Builder, ObjectBuilder<Property>> cityFn =
				fn -> fn.nested(
						city -> city.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
								.properties("district", districtFn));

		Function<Builder, ObjectBuilder<Property>> provinceFn = fn -> fn.nested(
				province -> province.properties("id",
								id -> id.long_(longProperty -> longProperty.index(true)))
						.properties("city", cityFn));

		Function<Builder, ObjectBuilder<Property>> regionFn = fn ->
				fn.nested(region -> region.properties("id",
								id -> id.long_(longProperty -> longProperty.index(true)))
						.properties("province", provinceFn));

		TypeMapping typeMapping = new TypeMapping.Builder()
				.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
				.properties("region", regionFn)
				.build();

		indexApi.createIndexWithMapping(indexName, typeMapping);

		// 创建数据
		Country country = Country.builder()
				.id(1L)
				.region(Region.builder()
						.id(1L)
						.province(Province.builder()
								.id(1L)
								.city(City.builder()
										.id(1L)
										.district(District.builder()
												.id(1L)
												.code(8)
												.name("test")
												.build())
										.build())
								.build())
						.build())
				.build();

		documentApi.addDocument(indexName, country);

		indexApi.refresh(indexName);
	}

	@AfterAll
	public void clear() throws IOException {
		indexApi.deleteIndex(indexName);

		indexName = null;
	}

	/**
	 * 嵌套查询, 内嵌文档查询
	 */
	@Test
	public void nestedQuery() throws IOException {
		// 准备查询
		Query query = Query.of(q -> q.bool(t -> t.must(List.of(
				Query.of(q1 -> q1.match(
						t1 -> t1.field("region.province.city.district.code").query(FieldValue.of(8)))),
				Query.of(q2 -> q2.match(
						t2 -> t2.field("region.province.city.district.name").query(FieldValue.of("test"))))))));

		List<Country> result = queryApi.nestedQuery(
				indexName,
				"region.province.city.district",
				query,
				ChildScoreMode.None,
				"id",
				0,
				10,
				true,
				Country.class);

		log.info("\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result),
				result.size());

		assertThat(result, notNullValue());
		assertThat(result.size(), equalTo(1));
	}
}
