package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.wujunshen.ApplicationTests;
import com.wujunshen.opensearch.api.IndexApi;
import com.wujunshen.opensearch.config.OpenSearchConfigProperties;
import java.io.IOException;
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
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.indices.GetIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br> email:<a
 * href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/19 10:30<br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(10)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class})//这里加启动类
class IndexApiTest {

	@Autowired
	private OpenSearchConfigProperties openSearchConfigProperties;

	@Autowired
	private IndexApi indexApi;

	private String indexName;

	@BeforeAll
	void setUp() {
		indexName = openSearchConfigProperties.getIndex();
	}

	@AfterAll
	void tearDown() {
		indexName = null;
	}

	/**
	 * 创建index
	 */
	@Order(3)
	@Test
	void createIndex() throws IOException {
		boolean isCreated = indexApi.createIndex(indexName);

		assertThat(isCreated, is(true));
	}

	/**
	 * 创建索引 - 指定mapping
	 */
	@Order(5)
	@Test
	void createIndexWithMapping() throws IOException {
		TypeMapping typeMapping = new TypeMapping.Builder().properties("sku",
						objectBuilder -> objectBuilder.text(textProperty -> textProperty.fielddata(true)))
				.properties("type",
						objectBuilder -> objectBuilder.text(textProperty -> textProperty.fielddata(true)))
				.properties("price", objectBuilder -> objectBuilder.double_(
						doubleNumberProperty -> doubleNumberProperty.index(true))).build();

		boolean isCreated = indexApi.createIndexWithMapping(indexName, typeMapping);

		assertThat(isCreated, is(true));
	}

	/**
	 * 创建索引 - 用json脚本创建mapping
	 */
	@Order(7)
	@Test
	void createIndexWithMappingWithScript() throws IOException {
		String mapping = """
				{
				  "properties": {
				    "id": {
				      "type": "long"
				    },
				    "user": {
				      "type": "nested",
				      "properties": {
				        "last": {
				          "type": "keyword"
				        },
				        "id": {
				          "type": "long"
				        },
				        "first": {
				          "type": "keyword"
				        }
				      }
				    },
				    "group": {
				      "fielddata": true,
				      "type": "text"
				    }
				  }
				}""";

		boolean isCreated = indexApi.createIndexWithMapping(indexName, mapping);

		assertThat(isCreated, is(true));
	}

	/**
	 * 查询index
	 */
	@Order(10)
	@Test
	void queryIndex() throws IOException {
		GetIndexResponse getIndexResponse = indexApi.queryIndex(indexName);

		assertThat(getIndexResponse, notNullValue());
	}

	/**
	 * 查询index相关信息
	 */
	@Order(15)
	@Test
	void queryIndexDetail() throws IOException {
		GetIndexResponse getIndexResponse = indexApi.queryIndexDetail(indexName);

		assertThat(getIndexResponse, notNullValue());
	}

	/**
	 * 判断index是否存在
	 */
	@Order(20)
	@Test
	void isExistedIndex() throws IOException {
		boolean isExisted = indexApi.isExistedIndex(indexName);

		assertThat(isExisted, is(true));
	}

	/**
	 * 获取所有index
	 */
	@Order(25)
	@Test
	public void getAllIndices() throws IOException {
		List<IndicesRecord> indicesRecords = indexApi.getAllIndices();

		for (IndicesRecord element : indicesRecords) {
			log.info("\nhealth:{}\nstatus:{}\nindexName:{}\nuuid:{}\npri:{}\nrep:{}\ndocsCount:{}",
					element.health(), element.status(), element.index(), element.uuid(), element.pri(),
					element.rep(), element.docsCount());
		}

		assertThat(indicesRecords, notNullValue());
	}

	/**
	 * 删除index
	 */
	@Order(80)
	@Test
	void deleteIndex() throws IOException {
		boolean isDeleted = indexApi.deleteIndex(indexName);

		assertThat(isDeleted, is(true));
	}
}
