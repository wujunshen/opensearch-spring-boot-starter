package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.product.Sku;
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
import org.opensearch.client.opensearch._types.aggregations.HistogramBucket;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/19 10:55<br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(30)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
class QueryApiTest {

  @Autowired private OpenSearchConfigProperties openSearchConfigProperties;

  @Autowired private DocumentApi documentApi;

  @Autowired private QueryApi queryApi;

  @Autowired private IndexApi indexApi;

  private Sku sku;

  private List<Sku> skuList;

  private String indexName;

  @BeforeAll
  void setUp() throws IOException {
    indexName = openSearchConfigProperties.getIndex();

    sku = Sku.builder().id(1L).skuName("City bike").skuPrice(123).build();

    skuList = bulkWriteProducts();

    TypeMapping typeMapping =
        new TypeMapping.Builder()
            .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
            .properties(
                "skuName", skuName -> skuName.text(textProperty -> textProperty.fielddata(true)))
            .properties(
                "skuPrice", skuPrice -> skuPrice.integer(intProperty -> intProperty.index(true)))
            .build();

    indexApi.createIndexWithMapping(indexName, typeMapping);

    documentApi.addDocument(indexName, String.valueOf(sku.getId()), sku);

    documentApi.batchAddDocument(indexName, skuList);

    indexApi.refresh(indexName);
  }

  @AfterAll
  void tearDown() throws IOException {
    sku = null;
    skuList = null;

    indexApi.deleteIndex(indexName);

    indexName = null;
  }

  /** aggregation operations */
  @Order(65)
  @Test
  void aggsByHistogram() throws IOException {
    List<HistogramBucket> buckets =
        queryApi.aggsByHistogram(indexName, "bike", "skuName", "skuPrice", "price-histogram", 50.0);

    for (HistogramBucket bucket : buckets) {
      log.info("There are " + bucket.docCount() + " bikes under " + bucket.key());
    }

    assertThat(buckets, notNullValue());
  }

  /** specify id to retrieve data */
  @Order(70)
  @Test
  void searchById() throws IOException {
    GetResponse<Sku> response =
        queryApi.searchById(indexName, String.valueOf(sku.getId()), Sku.class);

    if (response != null) {
      Sku source = response.source();
      log.info("sku: {}", source);
    }

    assertThat(response, notNullValue());
  }

  private List<Sku> bulkWriteProducts() {
    List<Sku> result = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String type = "City bike " + i;
      int price = (int) (Math.random() * 3 * 100);
      result.add(Sku.builder().id((long) i).skuName(type).skuPrice(price).build());
    }
    return result;
  }
}
