package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.usergroup.User;
import com.wujunshen.entity.usergroup.UserGroup;
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
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2020/2/7 5:33 下午 <br>
 */
@Slf4j
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Order(100)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
public class UserGroupQueryTest {

  private final ObjectMapper mapper = new ObjectMapper();
  @Autowired private OpenSearchConfigProperties openSearchConfigProperties;
  @Autowired private IndexApi indexApi;
  @Autowired private DocumentApi documentApi;
  @Autowired private QueryApi queryApi;
  private String indexName;

  @BeforeAll
  public void init() throws IOException {
    indexName = openSearchConfigProperties.getIndex();

    if (indexApi.isExistedIndex(indexName)) {
      indexApi.deleteIndex(indexName);
    }

    Function<Builder, ObjectBuilder<Property>> userFn =
        fn ->
            fn.nested(
                user ->
                    user.properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties(
                            "first",
                            first -> first.keyword(textProperty -> textProperty.index(true)))
                        .properties(
                            "last",
                            last -> last.keyword(textProperty -> textProperty.index(true))));

    TypeMapping typeMapping =
        new TypeMapping.Builder()
            .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
            .properties("group", group -> group.text(textProperty -> textProperty.fielddata(true)))
            .properties("user", userFn)
            .build();

    indexApi.createIndexWithMapping(indexName, typeMapping);

    // create Data
    User user = new User();
    user.setId(1L);
    user.setFirst("junshen");
    user.setLast("wu");

    UserGroup userGroup = new UserGroup();
    userGroup.setId(1L);
    userGroup.setGroup("fans");
    userGroup.setUser(user);

    documentApi.addDocument(indexName, userGroup);

    indexApi.refresh(indexName);
  }

  @AfterAll
  public void clear() throws IOException {
    indexApi.deleteIndex(indexName);

    indexName = null;
  }

  /** nested queries, inline document queries */
  @Test
  public void nestedQuery() throws IOException {
    // prepare query
    Query query =
        Query.of(
            q ->
                q.bool(
                    t ->
                        t.must(
                            List.of(
                                Query.of(
                                    q1 ->
                                        q1.match(
                                            t1 ->
                                                t1.field("user.first")
                                                    .query(FieldValue.of("junshen")))),
                                Query.of(
                                    q2 ->
                                        q2.match(
                                            t2 ->
                                                t2.field("user.last")
                                                    .query(FieldValue.of("wu"))))))));

    List<UserGroup> result =
        queryApi.nestedQuery(
            indexName, "user", query, ChildScoreMode.None, "id", 0, 10, true, UserGroup.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(1));
  }
}
