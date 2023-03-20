package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.product.Sku;
import com.wujunshen.entity.product.Spu;
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
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2020/2/7 5:33 4下午 <br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(40)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
public class OSCreateTest {

  @Autowired private OpenSearchConfigProperties openSearchConfigProperties;

  @Autowired private IndexApi indexApi;

  @Autowired private DocumentApi documentApi;

  private Spu spu1;
  private Spu spu2;

  private List<Spu> spuList = new ArrayList<>();

  private String indexName;

  @BeforeAll
  public void init() {
    indexName = openSearchConfigProperties.getIndex();

    spu1 = new Spu();
    spu1.setId(1L);
    spu1.setProductCode("7b28c293-4d06-4893-aad7-e4b6ed72c260");
    spu1.setProductName("android mobile");
    spu1.setBrandCode("H-001");
    spu1.setBrandName("HuaWei Nexus");
    spu1.setCategoryCode("C-001");
    spu1.setCategoryName("mobile");

    Sku sku1 = new Sku();
    sku1.setId(1L);
    sku1.setSkuCode("001");
    sku1.setSkuName("HuaWei Nexus P6");
    sku1.setSkuPrice(4000);
    sku1.setColor("Red");
    sku1.setSkuPrice(123);

    Sku sku2 = new Sku();
    sku2.setId(2L);
    sku2.setSkuCode("002");
    sku2.setSkuName("HuaWei P8");
    sku2.setSkuPrice(3000);
    sku2.setColor("Blank");
    sku2.setSkuPrice(456);

    Sku sku3 = new Sku();
    sku3.setId(3L);
    sku3.setSkuCode("003");
    sku3.setSkuName("HuaWei Nexus P6 Next Generation");
    sku3.setSkuPrice(5000);
    sku3.setColor("White");
    sku3.setSkuPrice(789);

    spu1.getSkus().add(sku1);
    spu1.getSkus().add(sku2);
    spu1.getSkus().add(sku3);

    spu2 = new Spu();
    spu2.setId(2L);
    spu2.setProductCode("AVYmdpQ_cnzgjoSZ6ent");
    spu2.setProductName("sportswear");
    spu2.setBrandCode("YD-001");
    spu2.setBrandName("LiNing");
    spu2.setCategoryCode("YDC-001");
    spu2.setCategoryName("dress");

    Sku sku21 = new Sku(21L, "YD001", "李宁衣服1", "Green", "2XL", 4000);
    Sku sku22 = new Sku(22L, "YD002", "李宁衣服2", "Green", "L", 3000);
    Sku sku23 = new Sku(23L, "YD003", "李宁衣服3", "Green", "M", 5000);

    spu2.getSkus().add(sku21);
    spu2.getSkus().add(sku22);
    spu2.getSkus().add(sku23);

    spuList.add(spu1);
    spuList.add(spu2);
  }

  @AfterAll
  public void clear() {
    indexName = null;

    spu1 = null;
    spu2 = null;

    spuList = null;
  }

  /**
   * create index
   *
   * @throws IOException exception info
   */
  @Order(0)
  @Test
  public void createIndex() throws IOException {
    assertThat(indexApi.isExistedIndex(indexName), is(false));

    if (!indexApi.isExistedIndex(indexName)) {
      assertThat(indexApi.createIndex(indexName), is(true));
    }

    assertThat(indexApi.isExistedIndex(indexName), is(true));
  }

  /**
   * delete index
   *
   * @throws IOException exception info
   */
  @Order(50)
  @Test
  public void deleteIndex() throws IOException {
    if (indexApi.isExistedIndex(indexName)) {
      assertThat(indexApi.deleteIndex(indexName), is(true));
    }

    assertThat(indexApi.isExistedIndex(indexName), is(false));
  }

  /**
   * single document insertion
   *
   * @throws IOException exception info
   */
  @Order(30)
  @Test
  public void operateData() throws IOException {
    documentApi.addDocument(indexName, String.valueOf(spu1.getId()), spu1);

    GetResponse<Spu> getResponse =
        documentApi.getDocument(indexName, String.valueOf(spu1.getId()), Spu.class);

    assertThat(getResponse.id(), notNullValue());
    assertThat(getResponse.id(), is("1"));

    DeleteResponse deleteResponse =
        documentApi.deleteDocument(indexName, String.valueOf(spu1.getId()));

    assertThat(deleteResponse.id(), notNullValue());
    assertThat(deleteResponse.id(), is("1"));
  }

  /**
   * batch document insertion
   *
   * @throws IOException exception info
   */
  @Order(40)
  @Test
  public void operateBatchData() throws IOException {
    boolean result = documentApi.batchAddDocument(indexName, spuList);

    log.info("batch insert operation: {}", result);

    assertThat(result, is(true));
  }
}
