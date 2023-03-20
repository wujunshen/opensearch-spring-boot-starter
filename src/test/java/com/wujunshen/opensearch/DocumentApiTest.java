package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.product.Sku;
import com.wujunshen.opensearch.api.DocumentApi;
import com.wujunshen.opensearch.api.IndexApi;
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
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.UpdateResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/19 10:17<br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
@Order(20)
class DocumentApiTest {

  @Autowired private OpenSearchConfigProperties openSearchConfigProperties;

  @Autowired private IndexApi indexApi;

  @Autowired private DocumentApi documentApi;

  private Sku sku;

  private Sku updateSku;

  private List<Sku> skuList;

  private String indexName;

  @BeforeAll
  void setUp() throws IOException {
    indexName = openSearchConfigProperties.getIndex();

    sku = Sku.builder().id(1L).skuName("City bike").skuPrice(123).build();

    updateSku = sku;
    updateSku.setSkuName("updated bike");
    updateSku.setSkuPrice(199);

    skuList = bulkWriteSkus();

    TypeMapping typeMapping =
        new TypeMapping.Builder()
            .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
            .properties(
                "skuName", skuName -> skuName.text(textProperty -> textProperty.fielddata(true)))
            .properties(
                "skuPrice", skuPrice -> skuPrice.integer(intProperty -> intProperty.index(true)))
            .build();

    indexApi.createIndexWithMapping(indexName, typeMapping);

    indexApi.refresh(indexName);
  }

  @AfterAll
  void tearDown() throws IOException {
    sku = null;
    updateSku = null;
    skuList = null;

    indexApi.deleteIndex(indexName);

    indexName = null;
  }

  /** write the single document */
  @Order(30)
  @Test
  void addDocument() throws IOException {
    IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

    String index = indexResponse.index();
    log.info("Indexed: {}", index);

    assertThat(index, equalTo(indexName));

    indexResponse = documentApi.addDocument(indexName, String.valueOf(sku.getId()), sku);

    index = indexResponse.index();
    log.info("Indexed: {}", index);

    assertThat(index, equalTo(indexName));
  }

  /** update document information */
  @Order(35)
  @Test
  void updateDocument() throws IOException {
    IndexResponse indexResponse = documentApi.addDocument(indexName, updateSku);

    UpdateResponse<Sku> updateResponse =
        documentApi.updateDocument(indexName, updateSku, indexResponse.id(), Sku.class);

    String index = updateResponse.index();
    log.info("Indexed: {}", index);

    assertThat(index, equalTo(indexName));
  }

  /** query document information */
  @Order(40)
  @Test
  void getDocument() throws IOException {
    IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

    GetResponse<Sku> getResponse =
        documentApi.getDocument(indexName, indexResponse.id(), Sku.class);

    String index = getResponse.index();
    log.info("Indexed: {}", index);

    assertThat(index, equalTo(indexName));
  }

  /** delete document information */
  @Order(45)
  @Test
  void deleteDocument() throws IOException {
    IndexResponse indexResponse = documentApi.addDocument(indexName, sku);

    DeleteResponse deleteResponse = documentApi.deleteDocument(indexName, indexResponse.id());

    String index = deleteResponse.index();
    log.info("Indexed: {}", index);

    assertThat(index, equalTo(indexName));
  }

  /** batch document insertion */
  @Order(50)
  @Test
  void batchAddDocument() throws IOException {
    boolean result = documentApi.batchAddDocument(indexName, skuList);

    log.info("batch insert operation: {}", result);

    assertThat(result, is(true));
  }

  /** delete all document information */
  @Order(55)
  @Test
  void deleteAllDocument() throws IOException {
    documentApi.batchAddDocument(indexName, skuList);
    indexApi.refresh(indexName);

    boolean result = documentApi.deleteAllDocument(indexName, Sku.class);

    log.info("batch insert operation: {}", result);

    assertThat(result, is(true));
  }

  /** delete all document information */
  @Order(57)
  @Test
  void batchDeleteDocument() throws IOException {
    documentApi.batchAddDocument(indexName, skuList);
    indexApi.refresh(indexName);

    List<String> ids = skuList.stream().map(e -> String.valueOf(e.getId())).toList();

    boolean result = documentApi.batchDeleteDocument(indexName, ids);

    log.info("batch insert operation: {}", result);

    assertThat(result, is(true));
  }

  private List<Sku> bulkWriteSkus() {
    List<Sku> result = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      String type = "City bike " + i;
      int price = (int) (Math.random() * 3 * 100);
      result.add(Sku.builder().id((long) i).skuName(type).skuPrice(price).build());
    }
    return result;
  }
}
