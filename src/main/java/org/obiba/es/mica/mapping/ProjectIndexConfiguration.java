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

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

public class ProjectIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(ProjectIndexConfiguration.class);

  public ProjectIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.DRAFT_PROJECT_INDEX.equals(indexName) ||
        Indexer.PUBLISHED_PROJECT_INDEX.equals(indexName)) {

      try {
        getClient(searchEngineService)
          .indices()
          .putMapping(new PutMappingRequest(indexName).source(createMappingProperties()), RequestOptions.DEFAULT);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(Indexer.PROJECT_TYPE);

    // properties
    mapping.startObject("properties");
    mapping.startObject("id").field("type", "keyword").endObject();
    appendMembershipProperties(mapping);
    Stream.of(Indexer.PROJECT_LOCALIZED_ANALYZED_FIELDS)
        .forEach(field -> createLocalizedMappingWithAnalyzers(mapping, field));
    mapping.endObject();

    mapping.endObject().endObject();
    return mapping;
  }

}
