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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilder;
import org.obiba.es.mica.query.AndQuery;
import org.obiba.es.mica.query.RQLJoinQuery;
import org.obiba.es.mica.query.RQLQuery;
import org.obiba.es.mica.results.ESResponseCountResults;
import org.obiba.es.mica.results.ESResponseDocumentResults;
import org.obiba.es.mica.support.AggregationParser;
import org.obiba.es.mica.support.ESHitSourceMapHelper;
import org.obiba.mica.spi.search.QueryScope;
import org.obiba.mica.spi.search.Searcher;
import org.obiba.mica.spi.search.support.EmptyQuery;
import org.obiba.mica.spi.search.support.JoinQuery;
import org.obiba.mica.spi.search.support.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.GlobalAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ExistsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.IdsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.PrefixQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.SourceFilter;
import co.elastic.clients.elasticsearch.core.search.TrackHits;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.obiba.mica.spi.search.QueryScope.AGGREGATION;
import static org.obiba.mica.spi.search.QueryScope.DETAIL;

public class ESSearcher implements Searcher {

  private static final Logger log = LoggerFactory.getLogger(Searcher.class);

  private final ESSearchEngineService esSearchService;

  private final AggregationParser aggregationParser = new AggregationParser();

  private final ObjectMapper objectMapper;

  ESSearcher(ESSearchEngineService esSearchService) {
    this(esSearchService, 250 * 1024 * 1024);
  }

  ESSearcher(ESSearchEngineService esSearchService, int bufferLimitBytes) {
    this.esSearchService = esSearchService;
    objectMapper = esSearchService.getObjectMapper();

    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    builder.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory
            .HeapBufferedResponseConsumerFactory(bufferLimitBytes));
  }

  @Override
  public JoinQuery makeJoinQuery(String rql) {
    log.debug("makeJoinQuery: {}", rql);
    RQLJoinQuery joinQuery = new RQLJoinQuery(esSearchService.getConfigurationProvider(), esSearchService.getIndexer());
    joinQuery.initialize(rql);
    return joinQuery;
  }

  @Override
  public Query makeQuery(String rql) {
    log.debug("makeQuery: {}", rql);
    if (Strings.isNullOrEmpty(rql)) return new EmptyQuery();
    return new RQLQuery(rql);
  }

  @Override
  public Query andQuery(Query... queries) {
    return new AndQuery(queries);
  }

  @Override
  public DocumentResults query(String indexName, String type, Query query, QueryScope scope, List<String> mandatorySourceFields, Properties aggregationProperties, @Nullable IdFilter idFilter) throws IOException {

    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);

    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    List<String> sourceFields = getSourceFields(query, mandatorySourceFields);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());

    TrackHits trackHits = new TrackHits.Builder().enabled(true).build();
    Aggregation globalAggregation = GlobalAggregation.of(g -> g)._toAggregation();
    SourceConfig.Builder sourceConfigBuilder = new SourceConfig.Builder();

    if (AGGREGATION == scope) {
      sourceConfigBuilder.fetch(false);
    } else if (sourceFields != null) {
      if (sourceFields.isEmpty()) sourceConfigBuilder.fetch(false);
      else sourceConfigBuilder.filter(SourceFilter.of(s -> s.includes(sourceFields).excludes(null)));
    }

    Map<String, Aggregation> aggregations = new HashMap<>();
    aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

    Map<String, Properties> subAggregationProperties = query.getAggregationBuckets().stream().collect(Collectors.toMap(b -> b, b -> aggregationProperties));
    Map<String, Aggregation> parsedAggregationsFromProperties = aggregationParser.getAggregations(aggregationProperties, subAggregationProperties);
    aggregations.putAll(parsedAggregationsFromProperties);

    co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

    SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
      .query(esQuery)
      .from(query.getFrom())
      .size(scope == DETAIL ? query.getSize() : 0)
      .trackTotalHits(trackHits)
      .source(sourceConfigBuilder.build())
      .aggregations(aggregations),
    ObjectNode.class);

    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, @Nullable IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);

    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());
    SearchResponse<ObjectNode> response = null;

    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build();
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = GlobalAggregation.of(g -> g)._toAggregation();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      Map<String, Properties> subAggregationProperties = query.getAggregationBuckets().stream().collect(Collectors.toMap(b -> b, b -> aggregationProperties));
      Map<String, Aggregation> parsedAggregationsFromProperties = aggregationParser.getAggregations(aggregationProperties, subAggregationProperties);
      aggregations.putAll(parsedAggregationsFromProperties);

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }

    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults cover(String indexName, String type, Query query, Properties aggregationProperties, Map<String, Properties> subAggregationProperties, @Nullable IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    // aggregationParser.getAggregations(aggregationProperties, subAggregationProperties).forEach(sourceBuilder::aggregation);

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());
    SearchResponse<ObjectNode> response = null;

    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build();
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = GlobalAggregation.of(g -> g)._toAggregation();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      // for (AbstractAggregationBuilder aggBuilder : aggregationParser.getAggregations(aggregationProperties, subAggregationProperties)) {
      //   aggregations.put(aggBuilder.getName(), new Aggregation.Builder().terms(agg -> agg.withJson(new StringReader(aggBuilder.toString()))).build());
      // }

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to cover {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
  }


  @Override
  public DocumentResults aggregate(String indexName, String type, Query query, Properties aggregationProperties, IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      TrackHits trackHits = new TrackHits.Builder().enabled(true).build();
      SourceConfig sourceConfig = new SourceConfig.Builder().fetch(false).build();
      Aggregation globalAggregation = GlobalAggregation.of(g -> g)._toAggregation();

      Map<String, Aggregation> aggregations = new HashMap<>();
      aggregations.put(AGG_TOTAL_COUNT, globalAggregation);

      Map<String, Aggregation> parsedAggregationsFromProperties = aggregationParser.getAggregations(aggregationProperties, null);
      aggregations.putAll(parsedAggregationsFromProperties);

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .trackTotalHits(trackHits)
        .source(sourceConfig)
        .aggregations(aggregations),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to aggregate {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);
    if (log.isTraceEnabled())
      log.trace("Response /{}/{}: totalHits={}", indexName, type, response == null ? 0 : response.hits().total().value());

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults find(String indexName, String type, String rql, IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);

    RQLQuery query = new RQLQuery(rql);
    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      List<SortOptions> sortOptions = new ArrayList<>();

      if (query.hasSortBuilders()) {
        for (SortBuilder sortBuilder : query.getSortBuilders()) {
          sortOptions.add(new SortOptions.Builder().withJson(new StringReader(sortBuilder.toString())).build());
        }
      } else {
        sortOptions.add(new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build());
      }

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(query.getFrom())
        .size(query.getSize())
        .sort(sortOptions),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to find {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults count(String indexName, String type, String rql, IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    RQLQuery query = new RQLQuery(rql);

    List<String> aggregations = query.getAggregations();
    if (query.getAggregations() !=  null && !aggregations.isEmpty()) {
      return countWithAggregations(indexName, type, rql, idFilter);
    }

    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();
    co.elastic.clients.elasticsearch._types.query_dsl.Query countQueryBuilder = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, countQueryBuilder.toString());
    CountResponse response = null;
    try {
      response = getClient().count(r -> r.index(indexName).query(countQueryBuilder));
    } catch (IOException e) {
      log.error("Failed to count {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseCountResults(response);
  }

  /**
   * Client code does not require a total count but a count per aggregation.
   *
   * @param indexName
   * @param type
   * @param rql
   * @param idFilter
   * @return
   */
  private DocumentResults countWithAggregations(String indexName, String type, String rql, IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = idFilter == null ? null : getIdQueryBuilder(idFilter);
    RQLQuery query = new RQLQuery(rql);
    co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder = query.isEmpty() || !query.hasQueryBuilder() ? new MatchAllQuery.Builder().build()._toQuery() : ((ESQuery) query).getQueryBuilder();

    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = filter == null ? queryBuilder : BoolQuery.of(q -> q.must(queryBuilder, filter))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      Map<String, Aggregation> aggregations = new HashMap<>();

      for (String field : query.getAggregations()) {
        aggregations.put(field, TermsAggregation.of(agg -> agg.field(field).size(Short.toUnsignedInt(Short.MAX_VALUE)))._toAggregation());
      }

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .aggregations(aggregations),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to count {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public List<String> suggest(String indexName, String type, int limit, String locale, String queryString, String defaultFieldNamePattern) {
    String localizedFieldName = String.format(defaultFieldNamePattern, locale);
    String fieldName = localizedFieldName.replace(".analyzed", "");

    co.elastic.clients.elasticsearch._types.query_dsl.Query query = QueryStringQuery.of(q -> q.query(queryString).defaultField(localizedFieldName).defaultOperator(Operator.Or))._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, query.toString());
    List<String> names = Lists.newArrayList();

    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = query;

      SourceConfig sourceConfig = new SourceConfig.Builder().filter(SourceFilter.of(s -> s.includes(fieldName))).build();
      SortOptions sortOption = new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(limit)
        .source(sourceConfig)
        .sort(sortOption),
      ObjectNode.class);

      response.hits().hits().forEach(hit -> {
          String value = ESHitSourceMapHelper.flattenMap(hit).get(fieldName).toLowerCase();
          names.add(Joiner.on(" ").join(Splitter.on(" ").trimResults().splitToList(value).stream()
            .filter(str -> !str.contains("[") && !str.contains("(") && !str.contains("{") && !str.contains("]") && !str.contains(")") && !str.contains("}"))
            .map(str -> str.replace(":", "").replace(",", ""))
            .filter(str -> !str.isEmpty()).collect(Collectors.toList())));
        }
      );

    } catch (IOException e) {
      log.error("Failed to suggest {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);


    return names;
  }

  @Override
  public InputStream getDocumentById(String indexName, String type, String id) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query query = IdsQuery.of(iq -> iq.values(id))._toQuery();

    log.debug("Request: /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, query.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = query;

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get document by ID {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.hits().total().value() == 0) return null;
    return new ByteArrayInputStream(response.hits().hits().get(0).toString().getBytes());
  }

  @Override
  public InputStream getDocumentByClassName(String indexName, String type, Class clazz, String id) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query classNameQuery = QueryStringQuery.of(q -> q.query(clazz.getSimpleName()).fields("className"))._toQuery();

    co.elastic.clients.elasticsearch._types.query_dsl.Query query = BoolQuery.of(q -> q.must(classNameQuery, IdsQuery.of(iq -> iq.values(id))._toQuery()))._toQuery(); 

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, query.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = query;

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get document by class name {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    if (response == null || response.hits().total().value() == 0) return null;
    return new ByteArrayInputStream(response.hits().hits().get(0).toString().getBytes());
  }

  @Override
  public DocumentResults getDocumentsByClassName(String indexName, String type, Class clazz, int from, int limit,
                                                 String sort, String order, String queryString,
                                                 TermFilter termFilter, IdFilter idFilter) {

    co.elastic.clients.elasticsearch._types.query_dsl.Query classNameQuery = QueryStringQuery.of(q -> q.query(clazz.getSimpleName()).fields("className"))._toQuery();   

    BoolQuery.Builder boolQuery = new BoolQuery.Builder().must(classNameQuery);

    if (queryString != null) {
      boolQuery.must(QueryStringQuery.of(q -> q.query(queryString))._toQuery());
    }

    co.elastic.clients.elasticsearch._types.query_dsl.Query postFilter = getPostFilter(termFilter, idFilter);

    co.elastic.clients.elasticsearch._types.query_dsl.Query execQuery = postFilter == null ? boolQuery.build()._toQuery() : boolQuery.must(postFilter).build()._toQuery();

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, execQuery.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = execQuery;

      String capitalizedOrder = order.substring(0, 1).toUpperCase() + order.substring(1).toLowerCase();

      SortOptions sortOption = sort != null ?
        new SortOptions.Builder().field(FieldSort.of(s -> s.field(sort).order(order == null ? co.elastic.clients.elasticsearch._types.SortOrder.Asc : co.elastic.clients.elasticsearch._types.SortOrder.valueOf(capitalizedOrder)))).build() :
        new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(from)
        .size(limit)
        .sort(sortOption),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get documents by class name{} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public DocumentResults getDocuments(String indexName, String type, int from, int limit, @Nullable String sort, @Nullable String order, @Nullable String queryString, @Nullable TermFilter termFilter, @Nullable IdFilter idFilter, @Nullable List<String> fields, @Nullable List<String> excludedFields) {
    QueryStringQuery.Builder query = queryString != null ? new QueryStringQuery.Builder().query(queryString) : null;
    if (query != null && fields != null) query.fields(fields);
    co.elastic.clients.elasticsearch._types.query_dsl.Query postFilter = getPostFilter(termFilter, idFilter);

    co.elastic.clients.elasticsearch._types.query_dsl.Query execQuery = postFilter == null ? (query == null ? new MatchAllQuery.Builder().build()._toQuery() : query.build()._toQuery()) : query == null ? postFilter : BoolQuery.of(q -> q.must(query.build()._toQuery()).filter(postFilter))._toQuery();

    if (excludedFields != null) {
      BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

      excludedFields.forEach(f -> boolQueryBuilder.mustNot(BoolQuery.of(q -> q.must(TermQuery.of(termQ -> termQ.field(f).value("true"))._toQuery(), ExistsQuery.of(existQ -> existQ.field(f))._toQuery()))._toQuery()));

      execQuery = boolQueryBuilder.must(execQuery).build()._toQuery();
    }

    log.debug("Request /{}/{}", indexName, type);
    if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, execQuery.toString());
    SearchResponse<ObjectNode> response = null;
    try {
      String capitalizedOrder = order.substring(0, 1).toUpperCase() + order.substring(1).toLowerCase();

      SortOptions sortOption = sort != null ?
        new SortOptions.Builder().field(FieldSort.of(s -> s.field(sort).order(order == null ? co.elastic.clients.elasticsearch._types.SortOrder.Asc : co.elastic.clients.elasticsearch._types.SortOrder.valueOf(capitalizedOrder)))).build() :
        new SortOptions.Builder().score(score -> score.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)).build();

      co.elastic.clients.elasticsearch._types.query_dsl.Query finalQuery = execQuery;

      response = getClient().search(s -> s.index(indexName)
        .query(finalQuery)
        .from(from)
        .size(limit)
        .sort(sortOption),
      ObjectNode.class);
    } catch (IOException e) {
      log.error("Failed to get documents {} - {}", indexName, e);
    }
    log.debug("Response /{}/{}", indexName, type);

    return new ESResponseDocumentResults(response, objectMapper);
  }

  @Override
  public long countDocumentsWithField(String indexName, String type, String field) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query theQuery = BoolQuery.of(q -> q.must(m -> m.exists(ExistsQuery.of(existsQ -> existsQ.field(field)))))._toQuery();

    try {
      log.debug("Request /{}/{}: {}", indexName, type, theQuery);
      if (log.isTraceEnabled()) log.trace("Request /{}/{}: {}", indexName, type, theQuery.toString());

      co.elastic.clients.elasticsearch._types.query_dsl.Query esQuery = theQuery;

      String cleanedField = field.replaceAll("\\.", "-");
      TermsAggregation termsAggregation = TermsAggregation.of(agg -> agg.field(cleanedField).size(Short.toUnsignedInt(Short.MAX_VALUE)));

      SearchResponse<ObjectNode> response = getClient().search(s -> s.index(indexName)
        .query(esQuery)
        .from(0)
        .size(0)
        .aggregations(cleanedField, termsAggregation._toAggregation()),
      ObjectNode.class);

      log.debug("Response /{}/{}: {}", indexName, type, response);

      return response.aggregations().get(cleanedField).sterms().buckets().array().stream().map(a -> a.key()).distinct().collect(Collectors.toList()).size();
    } catch (IndexNotFoundException | IOException e) {
      log.warn("Count of Studies With Variables failed", e);
      return 0;
    }
  }

  //
  // Private methods
  //

  private co.elastic.clients.elasticsearch._types.query_dsl.Query getPostFilter(TermFilter termFilter, IdFilter idFilter) {
    co.elastic.clients.elasticsearch._types.query_dsl.Query filter = null;

    if (idFilter != null) {
      filter = getIdQueryBuilder(idFilter);
    }

    if (termFilter != null && termFilter.getValue() != null) {
      TermQuery filterBy = TermQuery.of(q -> q.field(termFilter.getField()).value(termFilter.getValue()));
      filter = filter == null ? filterBy._toQuery() : null;
    }

    return filter;
  }

  private co.elastic.clients.elasticsearch._types.query_dsl.Query getIdQueryBuilder(IdFilter idFilter) {
    if (idFilter instanceof PathFilter) return getPathQueryBuilder((PathFilter) idFilter);

    Collection<String> ids = idFilter.getValues();

    if (ids.isEmpty()) {
      return BoolQuery.of(q -> q.mustNot(ExistsQuery.of(exQ -> exQ.field("id"))._toQuery()))._toQuery();
    } else if ("id".equals(idFilter.getField())) {
      return IdsQuery.of(q -> q.values(ids.stream().collect(Collectors.toList())))._toQuery();
    } else {
      List<co.elastic.clients.elasticsearch._types.query_dsl.Query> termQueries = ids.stream().map(id -> TermQuery.of(q -> q.field(idFilter.getField()).value(id))._toQuery()).collect(Collectors.toList());

      // FIXME filter = QueryBuilders.termsQuery(idFilter.getField(), ids);

      return BoolQuery.of(q -> q.should(termQueries))._toQuery();
    }
  }

  private co.elastic.clients.elasticsearch._types.query_dsl.Query getPathQueryBuilder(PathFilter pathFilter) {
    List<co.elastic.clients.elasticsearch._types.query_dsl.Query> includes = pathFilter.getValues().stream().map(path -> path.endsWith("/") ? PrefixQuery.of(q -> q.field(pathFilter.getField()).value(path))._toQuery() : TermQuery.of(q -> q.field(pathFilter.getField()).value(path))._toQuery()).collect(Collectors.toList());

    List<co.elastic.clients.elasticsearch._types.query_dsl.Query> excludes = pathFilter.getExcludedValues().stream().map(path -> PrefixQuery.of(q -> q.field(pathFilter.getField()).value(path))._toQuery()).collect(Collectors.toList());

    BoolQuery.Builder includedFilter = new BoolQuery.Builder();
    includes.forEach(includedFilter::should);
    if (excludes.isEmpty()) return includedFilter.build()._toQuery();

    BoolQuery.Builder excludedFilter = new BoolQuery.Builder();
    excludes.forEach(excludedFilter::should);

    return BoolQuery.of(q -> q.must(includedFilter.build()._toQuery(), excludedFilter.build()._toQuery()))._toQuery();
  }

  // private void appendAggregations(SearchSourceBuilder requestBuilder, List<String> aggregationBuckets, Properties aggregationProperties) {
  //   Map<String, Properties> subAggregations = Maps.newHashMap();
  //   if (aggregationBuckets != null)
  //     aggregationBuckets.forEach(field -> subAggregations.put(field, aggregationProperties));
  //   aggregationParser.setLocales(esSearchService.getConfigurationProvider().getLocales());
  //   aggregationParser.getAggregations(aggregationProperties, subAggregations).forEach(requestBuilder::aggregation);
  // }

  // private Iterable<AbstractAggregationBuilder> getAggregations(List<String> aggregationBuckets, Properties aggregationProperties) {
  //   Map<String, Properties> subAggregations = Maps.newHashMap();
  //   if (aggregationBuckets != null) aggregationBuckets.forEach(field -> subAggregations.put(field, aggregationProperties));
  //   aggregationParser.setLocales(esSearchService.getConfigurationProvider().getLocales());
  //   return aggregationParser.getAggregations(aggregationProperties, subAggregations);
  // }

  /**
   * Returns the default source filtering fields. A NULL signifies the whole source to be included
   */
  private List<String> getSourceFields(Query query, List<String> mandatorySourceFields) {
    List<String> sourceFields = query.getSourceFields();

    if (sourceFields != null && !sourceFields.isEmpty()) {
      sourceFields.addAll(mandatorySourceFields);
    }

    return sourceFields;
  }

  private ElasticsearchClient getClient() {
    return esSearchService.getClient();
  }

}
