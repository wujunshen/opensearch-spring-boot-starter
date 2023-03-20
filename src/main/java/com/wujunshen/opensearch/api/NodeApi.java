package com.wujunshen.opensearch.api;

import java.io.IOException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.nodes.NodesRecord;
import org.springframework.stereotype.Component;

/**
 * @author frank woo(吴峻申) <br>
 * @email <a href="mailto:frank_wjs@hotmail.com">frank_wjs@hotmail.com</a> <br>
 * @date 2022/8/28 20:26<br>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NodeApi {

  private final OpenSearchClient openSearchClient;

  /**
   * get all nodes information
   *
   * @return NodesRecord list
   * @throws IOException exception info
   */
  public List<NodesRecord> getAllNodes() throws IOException {
    List<NodesRecord> nodesRecords = openSearchClient.cat().nodes().valueBody();
    log.info("node size is:{}", nodesRecords.size());

    return nodesRecords;
  }
}
