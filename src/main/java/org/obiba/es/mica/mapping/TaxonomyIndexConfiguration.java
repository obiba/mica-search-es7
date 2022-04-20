/*
 * Copyright (c) 2018 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.mapping;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;

import co.elastic.clients.elasticsearch.indices.PutMappingRequest;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.Stream;

public class TaxonomyIndexConfiguration extends AbstractIndexConfiguration {

  private static final Logger log = LoggerFactory.getLogger(TaxonomyIndexConfiguration.class);

  public TaxonomyIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    try {
      XContentBuilder mapping = getMappingFromIndexName(indexName);

      if (mapping != null) {
        getClient(searchEngineService)
          .indices()
          .putMapping(PutMappingRequest.of(r -> r.index(indexName).withJson(new StringReader(Strings.toString(mapping)))));
      }
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private XContentBuilder getMappingFromIndexName(String indexName) throws IOException {
    switch (indexName) {
      case Indexer.TAXONOMY_INDEX:
        return createTaxonomyMappingProperties();
      case Indexer.VOCABULARY_INDEX:
        return createVocabularyMappingProperties();
      case Indexer.TERM_INDEX:
        return createTermMappingProperties();
      default:
        return null;  
    }
  }

  private XContentBuilder createTaxonomyMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject();
    return mapping;
  }

  private XContentBuilder createVocabularyMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createMappingWithAndWithoutAnalyzer(mapping, "taxonomyName");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject();
    return mapping;
  }

  private XContentBuilder createTermMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
    mapping.startObject("properties");
    createMappingWithoutAnalyzer(mapping, "id");
    createMappingWithoutAnalyzer(mapping, "target");
    createMappingWithAndWithoutAnalyzer(mapping, "name");
    createMappingWithAndWithoutAnalyzer(mapping, "taxonomyName");
    createMappingWithAndWithoutAnalyzer(mapping, "vocabularyName");
    Stream.of(Indexer.TAXONOMY_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject(); // properties
    mapping.endObject();
    return mapping;
  }

}
