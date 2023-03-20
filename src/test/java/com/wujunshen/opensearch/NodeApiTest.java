package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import com.wujunshen.ApplicationTests;
import com.wujunshen.opensearch.api.NodeApi;
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
import org.opensearch.client.opensearch.cat.nodes.NodesRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/28 20:29<br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(0)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
class NodeApiTest {

  @Autowired private NodeApi nodeApi;

  @BeforeAll
  void setUp() {}

  @AfterAll
  void tearDown() {}

  /** get all nodes information */
  @Order(0)
  @Test
  void getAllNodes() throws IOException {
    List<NodesRecord> nodesRecords = nodeApi.getAllNodes();

    for (NodesRecord nodesRecord : nodesRecords) {
      log.info("\nnodeName:{}\nip:{}", nodesRecord.name(), nodesRecord.ip());
    }

    assertThat(nodesRecords, notNullValue());
    assertThat(nodesRecords, hasSize(equalTo(3)));
  }
}
