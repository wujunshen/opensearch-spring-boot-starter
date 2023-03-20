package com.wujunshen.opensearch;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wujunshen.ApplicationTests;
import com.wujunshen.entity.foodtruck.FoodTruck;
import com.wujunshen.entity.foodtruck.Location;
import com.wujunshen.entity.foodtruck.Point;
import com.wujunshen.entity.foodtruck.TimeRange;
import com.wujunshen.opensearch.api.DocumentApi;
import com.wujunshen.opensearch.api.IndexApi;
import com.wujunshen.opensearch.api.QueryApi;
import com.wujunshen.opensearch.config.OpenSearchConfigProperties;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
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
import org.opensearch.client.json.JsonData;
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
@Order(300)
@ActiveProfiles(value = "local")
@TestInstance(Lifecycle.PER_CLASS)
@SpringBootTest(classes = {ApplicationTests.class}) // add startup class here
public class FoodTruckQueryTest {

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

    Function<Builder, ObjectBuilder<Property>> pointFn =
        fn ->
            fn.nested(
                point ->
                    point
                        .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties(
                            "lat", lat -> lat.double_(doubleProperty -> doubleProperty.index(true)))
                        .properties(
                            "lon",
                            lon -> lon.double_(doubleProperty -> doubleProperty.index(true))));

    Function<Builder, ObjectBuilder<Property>> timeRangeFn =
        fn ->
            fn.nested(
                timeRange ->
                    timeRange
                        .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties(
                            "from", from -> from.date(dateProperty -> dateProperty.index(true)))
                        .properties("to", to -> to.date(dateProperty -> dateProperty.index(true))));

    Function<Builder, ObjectBuilder<Property>> locationFn =
        fn ->
            fn.nested(
                location ->
                    location
                        .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
                        .properties(
                            "address",
                            address -> address.text(textProperty -> textProperty.fielddata(true)))
                        .properties("point", pointFn)
                        .properties("timeRange", timeRangeFn));

    TypeMapping typeMapping =
        new TypeMapping.Builder()
            .properties("id", id -> id.long_(longProperty -> longProperty.index(true)))
            .properties(
                "description",
                description -> description.text(textProperty -> textProperty.fielddata(true)))
            .properties("location", locationFn)
            .build();

    indexApi.createIndexWithMapping(indexName, typeMapping);

    // create Data
    FoodTruck foodTruck =
        FoodTruck.builder()
            .id(1L)
            .description("A very nice truck")
            .location(
                Location.builder()
                    .id(1L)
                    .address("Cologne City")
                    .point(new Point(1L, 50.9406645, 6.9599115))
                    .timeRange(
                        TimeRange.builder()
                            .id(1L)
                            .from(createTime(8, 30))
                            .to(createTime(12, 30))
                            .build())
                    .build())
            .build();

    documentApi.addDocument(indexName, foodTruck);

    indexApi.refresh(indexName);
  }

  @AfterAll
  public void clear() throws IOException {
    indexApi.deleteIndex(indexName);

    indexName = null;
  }

  /** nested queries, inline document queries */
  @Test
  public void nestedQueryPoint() throws IOException {
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
                                                t1.field("location.point.lat")
                                                    .query(FieldValue.of(50.9406645)))),
                                Query.of(
                                    q2 ->
                                        q2.range(
                                            t2 ->
                                                t2.field("location.point.lon")
                                                    .gt(JsonData.of(0.000))
                                                    .lt(JsonData.of(36.0000))))))));

    List<FoodTruck> result =
        queryApi.nestedQuery(
            indexName,
            "location.point",
            query,
            ChildScoreMode.None,
            "id",
            0,
            10,
            true,
            FoodTruck.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(1));

    query =
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
                                                t1.field("location.address")
                                                    .query(FieldValue.of("City"))))))));

    result =
        queryApi.nestedQuery(
            indexName, "location", query, ChildScoreMode.None, "id", 0, 10, true, FoodTruck.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(1));
  }

  /** nested queries, inline document queries */
  @Test
  public void nestedQueryTimeRange() throws IOException {
    // prepare query
    Query query =
        Query.of(
            q ->
                q.bool(
                    t ->
                        t.must(
                            Query.of(
                                q2 ->
                                    q2.range(
                                        t2 ->
                                            t2.field("location.timeRange.to")
                                                .gt(JsonData.of(createTime(12, 0).getTime()))
                                                .lt(JsonData.of(createTime(13, 0).getTime())))))));

    List<FoodTruck> result =
        queryApi.nestedQuery(
            indexName,
            "location.timeRange",
            query,
            ChildScoreMode.None,
            "id",
            0,
            10,
            true,
            FoodTruck.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(1));
  }

  @Test
  public void noNestedQuery() throws IOException {
    // prepare query
    List<FoodTruck> result =
        queryApi.fuzzyQuery(indexName, "truck", "description", "id", 0, 1, true, FoodTruck.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(1));

    result =
        queryApi.fuzzyQuery(indexName, "fuck", "description", "id", 0, 1, true, FoodTruck.class);

    log.info(
        "\njson string is:{}，list size is:{}\n", mapper.writeValueAsString(result), result.size());

    assertThat(result, notNullValue());
    assertThat(result.size(), equalTo(0));
  }

  private Date createTime(int hour, int minutes) {
    Calendar cal = Calendar.getInstance();

    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, minutes);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.set(Calendar.DATE, 0);
    cal.set(Calendar.MONTH, 0);
    cal.set(Calendar.YEAR, 0);

    return cal.getTime();
  }
}
