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

import org.obiba.mica.spi.search.IndexFieldMapping;
import org.obiba.mica.spi.search.Indexable;
import org.obiba.mica.spi.search.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Persistable;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BooleanResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringReader;
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
    IndexRequest<JsonData> indexRequest =
      getIndexRequestBuilder(indexName, persistable.getId(), toJson(persistable), parent == null ? null : parent.getId());

    try {
      getClient().index(indexRequest);
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
    IndexRequest<JsonData> indexRequest =
      getIndexRequestBuilder(indexName, indexable.getId(), toJson(indexable), parent == null ? null : parent.getId());

    try {
      getClient().index(indexRequest);
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

    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Persistable<String> persistable : persistables) {
      br.operations(op -> op.index(idx -> idx.index(indexName).id(persistable.getId()).document(persistable)));
    }

    try {
      getClient().bulk(br.build());
    } catch (IOException e) {
      log.error("Failed to bulk index {} - {}", indexName, e);
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

    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Indexable indexable: indexables) {
      br.operations(op -> op.index(idx -> idx.index(indexName).id(indexable.getId()).document(indexable)));
    }

    try {
      getClient().bulk(br.build());
    } catch (IOException e) {
      log.error("Failed to bulk index {} - {}", indexName, e);
    }
  }

  @Override
  public void delete(String indexName, Persistable<String> persistable) {
    createIndexIfNeeded(indexName);
    try {
      getClient().delete(DeleteRequest.of(r -> r.index(indexName).id(persistable.getId())));
    } catch (IOException e) {
      log.error("Failed to delete document in index {} - {}", persistable.getId(), indexName, e);
    }
  }

  @Override
  public void delete(String indexName, Indexable indexable) {
    createIndexIfNeeded(indexName);
    try {
      getClient().delete(DeleteRequest.of(r -> r.index(indexName).id(indexable.getId())));
    } catch (IOException e) {
      log.error("Failed to delete document in index {} - {}", indexable.getId(), indexName, e);
    }
  }

  @Override
  public void delete(String indexName, String[] types, Map.Entry<String, String> termQuery) {
    if (!hasIndex(indexName)) return;

    DeleteByQueryRequest deleteRequest = DeleteByQueryRequest.of(r -> r
      .index(indexName)
      .query(q -> q
        .term(t -> t
          .field(termQuery.getKey()).value(termQuery.getValue())
        )
      )
    );

    try {
      getClient().deleteByQuery(deleteRequest);
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
      BooleanResponse exists = getClient().indices().exists(ExistsRequest.of(r -> r.index(indexName)));
      return exists.value();
    } catch (IOException e) {
      log.error("Failed to find index {} - {}", indexName, e);
    }

    return false;
  }

  @Override
  public void dropIndex(String indexName) {
    try {
      getClient().indices().delete(DeleteIndexRequest.of(r -> r.index(indexName)));
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
      GetMappingResponse result = getClient().indices().getMapping(GetMappingRequest.of(r -> r.index(indexName)));
      Map<String, IndexMappingRecord> mappings = result.result();
      IndexMappingRecord record = mappings.get(indexName);
      String recordAsString = Configuration.defaultConfiguration().jsonProvider().toJson(record.mappings().meta());
      return JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST)).parse(recordAsString);

    } catch (IOException e) {
      log.error("Failed to drop index index {} - {}", indexName, e);
    }

    return null;
  }

  private ElasticsearchClient getClient() {
    return esSearchService.getClient();
  }

  private String toJson(Object obj) {
    try {
      return esSearchService.getObjectMapper().writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Cannot serialize " + obj + " to ElasticSearch", e);
    }
  }

  private IndexRequest<JsonData> getIndexRequestBuilder(String indexName, String id, String source, String parentId) {
    IndexRequest<JsonData> request = IndexRequest.of(r -> r.index(indexName).id(id).routing(parentId).withJson(new StringReader(source)));
    return request;
  }

  private synchronized CreateIndexResponse createIndexIfNeeded(String indexName) {
    log.trace("Ensuring index existence for index {}", indexName);
    ElasticsearchIndicesClient indicesAdmin = getClient().indices();

    if (!hasIndex(indexName)) {
      log.info("Creating index {}", indexName);

      IndexSettings.Builder indexSettingsBuilder = new IndexSettings.Builder();

      if (!esSearchService.getIndexSettings().equals("{}")) {
        indexSettingsBuilder.withJson(new StringReader(esSearchService.getIndexSettings()));
      }

      IndexSettings settings = indexSettingsBuilder
        .numberOfReplicas(Integer.toString(esSearchService.getNbReplicas()))
        .numberOfShards(Integer.toString(esSearchService.getNbShards())).build();
      try {
        CreateIndexResponse createdResponse = indicesAdmin.create(CreateIndexRequest.of(r -> r.index(indexName).settings(settings)));
        esSearchService.getIndexConfigurationListeners().forEach(listener -> listener.onIndexCreated(esSearchService, indexName));

        return createdResponse;
      } catch (IOException e) {
        log.error("Failed to create index index {} - {}", indexName, e);
      }
    }

    return null;
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
