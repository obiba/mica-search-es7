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

public class PersonIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(PersonIndexConfiguration.class);

  public PersonIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.PERSON_INDEX.equals(indexName)) {
      try {
        getClient(searchEngineService)
          .indices()
          .putMapping(new PutMappingRequest(indexName)
            .source(createMappingProperties(Indexer.PERSON_TYPE)), RequestOptions.DEFAULT);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties(String type) throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
    mapping.startObject("properties");
    mapping.startObject("id").field("type", "keyword").endObject();
    mapping.startObject("studyMemberships").startObject("properties");
    mapping.startObject("parentId").field("type", "keyword").endObject();
    mapping.startObject("role").field("type", "keyword").endObject();
    mapping.endObject().endObject();
    mapping.startObject("networkMemberships").startObject("properties");
    mapping.startObject("parentId").field("type", "keyword").endObject();
    mapping.startObject("role").field("type", "keyword").endObject();
    mapping.endObject().endObject();
    mapping.startObject("institution");
    mapping.startObject("properties");
    createLocalizedMappingWithAnalyzers(mapping, "name");
    mapping.endObject().endObject();

    createMappingWithAndWithoutAnalyzer(mapping, "firstName");
    createMappingWithAndWithoutAnalyzer(mapping, "lastName");
    createMappingWithAndWithoutAnalyzer(mapping, "fullName");
    createMappingWithAndWithoutAnalyzer(mapping, "email");
    mapping.endObject(); // properties
    mapping.endObject();

    return mapping;
  }
}
