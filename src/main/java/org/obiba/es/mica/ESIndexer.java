/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.obiba.mica.spi.search.IndexFieldMapping;
import org.obiba.mica.spi.search.Indexable;
import org.obiba.mica.spi.search.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Persistable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESIndexer implements Indexer {

  private static final Logger log = LoggerFactory.getLogger(ESIndexer.class);

  private static final int MAX_SIZE = 10000;

  private final ESSearchEngineService esSearchService;

  public ESIndexer(ESSearchEngineService esSearchService) {
    this.esSearchService = esSearchService;
  }

  @Override
  public void index(String indexName, Persistable<String> persistable) {
    index(indexName, persistable, null);
  }

  @Override
  public void index(String indexName, Persistable<String> persistable, Persistable<String> parent) {
    log.debug("Indexing for indexName [{}] indexableObject [{}]", indexName, persistable);
    createIndexIfNeeded(indexName);
    IndexRequest indexRequest =
      getIndexRequestBuilder(indexName, persistable.getId(), toJson(persistable), parent == null ? null : parent.getId());

    try {
      getClient().index(indexRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to index {} in index {} - {}", persistable.getId(), indexName, e);
    }
  }

  @Override
  public void index(String indexName, Indexable indexable) {
    index(indexName, indexable, null);
  }

  @Override
  public void index(String indexName, Indexable indexable, Indexable parent) {
    log.debug("Indexing for indexName [{}] indexableObject [{}]", indexName, indexable);
    createIndexIfNeeded(indexName);
    IndexRequest indexRequest =
      getIndexRequestBuilder(indexName, indexable.getId(), toJson(indexable), parent == null ? null : parent.getId());

    try {
      getClient().index(indexRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to index {} in index {} - {}", indexable.getId(), indexName, e);
    }
  }

  @Override
  synchronized public void reIndexAllIndexables(String indexName, Iterable<? extends Indexable> persistables) {
    if (hasIndex(indexName)) dropIndex(indexName);
    indexAllIndexables(indexName, persistables, null);
  }

  @Override
  synchronized public void reindexAll(String indexName, Iterable<? extends Persistable<String>> persistables) {
    if (hasIndex(indexName)) dropIndex(indexName);
    indexAll(indexName, persistables, null);
  }

  @Override
  public void indexAll(String indexName, Iterable<? extends Persistable<String>> persistables) {
    indexAll(indexName, persistables, null);
  }

  @Override
  public void indexAll(String indexName, Iterable<? extends Persistable<String>> persistables, Persistable<String> parent) {

    log.debug("Indexing all for indexName [{}] persistableObjectNumber [{}]", indexName, Iterables.size(persistables));

    createIndexIfNeeded(indexName);
    BulkRequest bulkRequest = new BulkRequest();
    persistables.forEach(persistable ->
      bulkRequest.add(
        getIndexRequestBuilder(
          indexName,
          persistable.getId(),
          toJson(persistable),
          parent == null ? null : parent.getId()
        ))
    );

    if (bulkRequest.numberOfActions() > 0) {
      try {
        getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        log.error("Failed to bulk index {} - {}", indexName, e);
      }
    }
  }

  @Override
  public void indexAllIndexables(String indexName, Iterable<? extends Indexable> indexables) {
    indexAllIndexables(indexName, indexables, null);
  }

  @Override
  public void indexAllIndexables(String indexName, Iterable<? extends Indexable> indexables, @Nullable String parentId) {
    log.debug("Indexing all indexables for indexName [{}] persistableObjectNumber [{}]", indexName, Iterables.size(indexables));
    createIndexIfNeeded(indexName);
    BulkRequest bulkRequest = new BulkRequest();
    indexables.forEach(indexable ->
      bulkRequest.add(getIndexRequestBuilder(indexName, indexable.getId(), toJson(indexable), parentId)));

    if (bulkRequest.numberOfActions() > 0) {
      try {
        getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
      } catch (IOException e) {
        log.error("Failed to bulk index {} - {}", indexName, e);
      }
    }
  }

  @Override
  public void delete(String indexName, Persistable<String> persistable) {
    createIndexIfNeeded(indexName);
    DeleteRequest deleteRequest = new DeleteRequest(indexName, persistable.getId());
    try {
      getClient().delete(deleteRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to delete document in index {} - {}", persistable.getId(), indexName, e);
    }
  }

  @Override
  public void delete(String indexName, Indexable indexable) {
    createIndexIfNeeded(indexName);
    DeleteRequest deleteRequest = new DeleteRequest(indexName, indexable.getId());
    try {
      getClient().delete(deleteRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to delete document in index {} - {}", indexable.getId(), indexName, e);
    }
  }

  @Override
  public void delete(String indexName, String[] types, Map.Entry<String, String> termQuery) {
    if (!hasIndex(indexName)) return;

    QueryBuilder query = QueryBuilders.termQuery(termQuery.getKey(), termQuery.getValue());

    try {
      getClient().deleteByQuery(new DeleteByQueryRequest(indexName).setQuery(query), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to delete document by query in index {} - {}", indexName, e);
    }
  }

  @Override
  public void delete(String indexName, String type, Map.Entry<String, String> termQuery) {
    delete(indexName, type != null ? new String[]{type} : null, termQuery);
  }

  @Override
  public boolean hasIndex(String indexName) {
    try {
      return getClient().indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to find index {} - {}", indexName, e);
    }

    return false;
  }

  @Override
  public void dropIndex(String indexName) {
    try {
      getClient().indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to drop index index {} - {}", indexName, e);
    }
  }

  @Override
  public IndexFieldMapping getIndexfieldMapping(String indexName, String type) {
    return new IndexFieldMappingImpl(hasIndex(indexName) ? getContext(indexName, type) : null);
  }

  //
  // Private methods
  //

  private ReadContext getContext(String indexName, String indexType) {
    try {
      GetMappingsResponse result = getClient().indices().getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT);
      Map<String, MappingMetadata> mappings = result.mappings();
      MappingMetadata metaData = mappings.get(indexName);
      Object jsonContent = Configuration.defaultConfiguration().jsonProvider().parse(metaData.source().toString());
      return JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST)).parse(jsonContent);

    } catch (IOException e) {
      log.error("Failed to drop index index {} - {}", indexName, e);
    }

    return null;
  }

  private RestHighLevelClient getClient() {
    return esSearchService.getClient();
  }

  private String toJson(Object obj) {
    try {
      return esSearchService.getObjectMapper().writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot serialize " + obj + " to ElasticSearch", e);
    }
  }

  private IndexRequest getIndexRequestBuilder(String indexName, String id, String source, String parentId) {
    IndexRequest request = new IndexRequest(indexName)
      .id(id)
      .source(source, XContentType.JSON);

    if (parentId != null) {
      request.routing(parentId);
    }

    return request;
  }

  private synchronized void createIndexIfNeeded(String indexName) {
    log.trace("Ensuring index existence for index {}", indexName);
    IndicesClient indicesAdmin = getClient().indices();

    if (!hasIndex(indexName)) {
      log.info("Creating index {}", indexName);

      Settings settings = Settings.builder() //
          .put(esSearchService.getIndexSettings())
          .put("number_of_shards", esSearchService.getNbShards()) //
          .put("number_of_replicas", esSearchService.getNbReplicas()).build();

      try {
        indicesAdmin.create(new CreateIndexRequest(indexName).settings(settings), RequestOptions.DEFAULT);
        esSearchService.getIndexConfigurationListeners().forEach(listener -> listener.onIndexCreated(esSearchService, indexName));
      } catch (IOException e) {
        log.error("Failed to create index index {} - {}", indexName, e);
      }
    }
  }

  private static class IndexFieldMappingImpl implements IndexFieldMapping {

    private final ReadContext context;

    IndexFieldMappingImpl(ReadContext ctx) {
      this.context = ctx;
    }

    @Override
    public boolean isAnalyzed(String fieldName) {
      boolean analyzed = false;
      if (context != null) {
        List<Object> result = context.read(String.format("$..%s..analyzed", fieldName.replaceAll("\\.", "..")));
        analyzed = result.size() > 0;
      }

      return analyzed;
    }

  }

}
