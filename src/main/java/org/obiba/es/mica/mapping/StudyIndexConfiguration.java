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

import com.google.common.collect.Lists;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.obiba.mica.spi.search.TaxonomyTarget;
import org.obiba.opal.core.domain.taxonomy.Taxonomy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.indices.PutMappingRequest;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class StudyIndexConfiguration extends AbstractIndexConfiguration {
  private static final Logger log = LoggerFactory.getLogger(StudyIndexConfiguration.class);

  public StudyIndexConfiguration(ConfigurationProvider configurationProvider) {
    super(configurationProvider);
  }

  @Override
  public void onIndexCreated(SearchEngineService searchEngineService, String indexName) {
    if (Indexer.DRAFT_STUDY_INDEX.equals(indexName) || Indexer.PUBLISHED_STUDY_INDEX.equals(indexName)) {

      try {
        XContentBuilder properties = createMappingProperties();

        getClient(searchEngineService)
          .indices()
          .putMapping(PutMappingRequest.of(r -> r.index(indexName).withJson(new StringReader(Strings.toString(properties)))));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private XContentBuilder createMappingProperties() throws IOException {
    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();

    mapping.startObject("properties");
    Taxonomy taxonomy = getTaxonomy();
    addLocalizedVocabularies(taxonomy, "name", "acronym");
    List<String> ignore = Lists.newArrayList(
      "id"
    );
    addTaxonomyFields(mapping, taxonomy, ignore);
    mapping.endObject().endObject();

    return mapping;
  }

  @Override
  protected TaxonomyTarget getTarget() {
    return TaxonomyTarget.STUDY;
  }
}
