/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weasel.elasticsearch.core;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.elasticsearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.elasticsearch.client.Requests.indicesExistsRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.index.VersionType.EXTERNAL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.weasel.lang.EsPage;
import com.weasel.lang.Page;
import com.weasel.lang.annotation.Document;
import com.weasel.lang.helper.GodHands;
import com.weasel.lang.helper.JsonHelper;
import com.weasel.elasticsearch.core.query.DeleteQuery;
import com.weasel.elasticsearch.core.query.GetQuery;
import com.weasel.elasticsearch.core.query.IndexQuery;
import com.weasel.elasticsearch.core.query.MoreLikeThisQuery;
import com.weasel.elasticsearch.core.query.Query;
import com.weasel.elasticsearch.core.query.SearchQuery;
import com.weasel.elasticsearch.core.query.SuggestQuery;
import com.weasel.elasticsearch.core.query.UpdateQuery;
import com.weasel.elasticsearch.exception.ElasticsearchException;

/**
 * ElasticsearchTemplate
 * 
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Artur Konczak
 * @author Dylan
 */
@Repository
public class ElasticsearchOperations implements ElasticsearchRepository {

	@Autowired
	protected Client es;

	public Client getEs() {
		return es;
	}

	public void setEs(Client es) {
		this.es = es;
	}

	@Override
	public <T> boolean createIndex(Class<T> clazz) {
		return createIndexIfNotCreated(clazz);
	}

	@Override
	public <T> T queryForObject(GetQuery query, Class<T> clazz) {
		GetResponse response = es.prepareGet(getIndexName(clazz), getType(clazz), query.getId()).execute().actionGet();
		if (StringUtils.isBlank(response.getSourceAsString()))
			return null;
		return JsonHelper.fromJsonString(response.getSourceAsString(), clazz);
	}

	@Override
	public <T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz) {
		SearchResponse response = doSearch(prepareSearch(query, clazz), query);
		return parseResult(response, clazz, query.getPageable(),null);
	}
	
	@Override
	public <T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz,HighlightFieldResultParser<T> parser) {
		SearchResponse response = doSearch(prepareSearch(query, clazz), query);
		return parseResult(response, clazz, query.getPageable(),parser);
	}

	@Override
	public <T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz, SearchResultParser<T> parser) {
		SearchResponse response = doSearch(prepareSearch(query, clazz), query);
		EsPage<T> facetPage = new EsPage<T>();
		Page<?> page = query.getPageable();
		SearchHits hits = response.getHits();
		long totalHits = hits.totalHits();
		facetPage.setTotalCount((int) totalHits);
		if (null != page) {
			facetPage.setPageSize(page.getPageSize()).setCurrentPage(page.getCurrentPage());
		}
		return parser.parseResult(response, clazz, facetPage);
	}
	
	@Override
	public <T> List<T> queryForList(SearchQuery query, Class<T> clazz) {
		return queryForPage(query, clazz).getResult();
	}

	@Override
	public <T> List<String> queryForIds(SearchQuery query) {
		SearchRequestBuilder request = prepareSearch(query).setQuery(query.getQuery()).setNoFields();
		if (query.getFilter() != null) {
			request.setFilter(query.getFilter());
		}
		SearchResponse response = request.execute().actionGet();
		return extractIds(response);
	}

	@Override
	public <T> long count(SearchQuery query, Class<T> clazz) {
		CountRequestBuilder countRequestBuilder = es.prepareCount(getIndexName(clazz)).setTypes(getType(clazz));
		if (query.getQuery() != null) {
			countRequestBuilder.setQuery(query.getQuery());
		}
		return countRequestBuilder.execute().actionGet().getCount();
	}

	@Override
	public String index(IndexQuery query) {
		return prepareIndex(query).execute().actionGet().getId();
	}

	@Override
	public UpdateResponse update(UpdateQuery query) {
		String indexName = isNotBlank(query.getIndexName()) ? query.getIndexName() : getIndexName(query.getClazz());
		String type = isNotBlank(query.getType()) ? query.getType() : getType(query.getClazz());
		Assert.notNull(indexName, "No index defined for Query");
		Assert.notNull(type, "No type define for Query");
		Assert.notNull(query.getId(), "No Id define for Query");
		Assert.notNull(query.getIndexRequest(), "No IndexRequest define for Query");
		UpdateRequestBuilder updateRequestBuilder = es.prepareUpdate(indexName, type, query.getId());
		if (query.DoUpsert()) {
			updateRequestBuilder.setDocAsUpsert(true).setUpsert(query.getIndexRequest()).setDoc(query.getIndexRequest());
		} else {
			updateRequestBuilder.setDoc(query.getIndexRequest());
		}
		return updateRequestBuilder.execute().actionGet();
	}

	@Override
	public void bulkIndex(List<IndexQuery> queries) {
		BulkRequestBuilder bulkRequest = es.prepareBulk();
		for (IndexQuery query : queries) {
			bulkRequest.add(prepareIndex(query));
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			Map<String, String> failedDocuments = new HashMap<String, String>();
			for (BulkItemResponse item : bulkResponse.getItems()) {
				if (item.isFailed())
					failedDocuments.put(item.getId(), item.getFailureMessage());
			}
			throw new ElasticsearchException("Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages [" + failedDocuments + "]", failedDocuments);
		}
	}

	@Override
	public String delete(String indexName, String type, String id) {
		return es.prepareDelete(indexName, type, id).execute().actionGet().getId();
	}

	@Override
	public <T> String delete(Class<T> clazz, String id) {
		return delete(getIndexName(clazz), getType(clazz), id);
	}

	@Override
	public <T> void delete(DeleteQuery query, Class<T> clazz) {
		es.prepareDeleteByQuery(getIndexName(clazz)).setTypes(getType(clazz)).setQuery(query.getQuery()).execute().actionGet();
	}

	@Override
	public void delete(DeleteQuery query) {
		Assert.notNull(query.getIndex(), "No index defined for Query");
		Assert.notNull(query.getType(), "No type define for Query");
		es.prepareDeleteByQuery(query.getIndex()).setTypes(query.getType()).setQuery(query.getQuery()).execute().actionGet();
	}

	@Override
	public <T> boolean deleteIndex(Class<T> clazz) {
		String indexName = getIndexName(clazz);
		if (indexExists(indexName)) {
			return es.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet().isAcknowledged();
		}
		return false;
	}

	@Override
	public void deleteType(String index, String type) {
		Map<String, MappingMetaData> mappings = es.admin().cluster().prepareState().execute().actionGet().getState().metaData().index(index).mappings();
		if (mappings.containsKey(type)) {
			es.admin().indices().deleteMapping(new DeleteMappingRequest(index).type(type)).actionGet();
		}
	}

	@Override
	public <T> boolean indexExists(Class<T> clazz) {
		return indexExists(getIndexName(clazz));
	}

	@Override
	public boolean typeExists(String index, String type) {
		return es.admin().cluster().prepareState().execute().actionGet().getState().metaData().index(index).mappings().containsKey(type);
	}

	@Override
	public void refresh(String indexName, boolean waitForOperation) {
		es.admin().indices().refresh(refreshRequest(indexName)).actionGet();

	}

	@Override
	public <T> void refresh(Class<T> clazz, boolean waitForOperation) {
		es.admin().indices().refresh(refreshRequest(getIndexName(clazz))).actionGet();
	}

	@Override
	public <T> Page<T> moreLikeThis(MoreLikeThisQuery query, Class<T> clazz) {
		int startRecord = 0;
		String indexName = isNotBlank(query.getIndexName()) ? query.getIndexName() : getIndexName(clazz);
		String type = isNotBlank(query.getType()) ? query.getType() : getType(clazz);

		Assert.notNull(indexName, "No 'indexName' defined for MoreLikeThisQuery");
		Assert.notNull(type, "No 'type' defined for MoreLikeThisQuery");
		Assert.notNull(query.getId(), "No document id defined for MoreLikeThisQuery");

		MoreLikeThisRequestBuilder requestBuilder = es.prepareMoreLikeThis(indexName, type, query.getId());

		if (query.getPageable() != null) {
			startRecord = query.getPageable().getCurrentPage() * query.getPageable().getPageSize();
			requestBuilder.setSearchSize(query.getPageable().getPageSize());
		}
		requestBuilder.setSearchFrom(startRecord);

		if (isNotEmpty(query.getSearchIndices())) {
			requestBuilder.setSearchIndices(toArray(query.getSearchIndices()));
		}
		if (isNotEmpty(query.getSearchTypes())) {
			requestBuilder.setSearchTypes(toArray(query.getSearchTypes()));
		}
		if (isNotEmpty(query.getFields())) {
			requestBuilder.setField(toArray(query.getFields()));
		}
		if (isNotBlank(query.getRouting())) {
			requestBuilder.setRouting(query.getRouting());
		}
		if (query.getPercentTermsToMatch() != null) {
			requestBuilder.setPercentTermsToMatch(query.getPercentTermsToMatch());
		}
		if (query.getMinTermFreq() != null) {
			requestBuilder.setMinTermFreq(query.getMinTermFreq());
		}
		if (query.getMaxQueryTerms() != null) {
			requestBuilder.maxQueryTerms(query.getMaxQueryTerms());
		}
		if (isNotEmpty(query.getStopWords())) {
			requestBuilder.setStopWords(toArray(query.getStopWords()));
		}
		if (query.getMinDocFreq() != null) {
			requestBuilder.setMinDocFreq(query.getMinDocFreq());
		}
		if (query.getMaxDocFreq() != null) {
			requestBuilder.setMaxDocFreq(query.getMaxDocFreq());
		}
		if (query.getMinWordLen() != null) {
			requestBuilder.setMinWordLen(query.getMinWordLen());
		}
		if (query.getMaxWordLen() != null) {
			requestBuilder.setMaxWordLen(query.getMaxWordLen());
		}
		if (query.getBoostTerms() != null) {
			requestBuilder.setBoostTerms(query.getBoostTerms());
		}

		SearchResponse response = requestBuilder.execute().actionGet();
		return parseResult(response, clazz, query.getPageable(),null);

	}

	/**
	 * @param suggestQuery
	 * @param clazz
	 * @return
	 */
	@Override
	public <T> SuggestResponse suggest(SuggestQuery suggestQuery, Class<T> clazz) {
		
		String[] indices = isNotEmpty(suggestQuery.getSearchIndices()) ? toArray(suggestQuery.getSearchIndices()) : new String[] { getIndexName(clazz) };
		SuggestRequestBuilder suggestRequestBuilder = es.prepareSuggest(indices);
		if (isNotBlank(suggestQuery.getPreference())) {
			suggestRequestBuilder.setPreference(suggestQuery.getPreference());
		}
		
		if (isNotBlank(suggestQuery.getSuggestText())) {
			suggestRequestBuilder.setSuggestText(suggestQuery.getSuggestText());
		}
		
		if (isNotEmpty(suggestQuery.getRouting())) {
			suggestRequestBuilder.setRouting(toArray(suggestQuery.getRouting()));
		}
		
		if (isNotEmpty(suggestQuery.getSearchIndices())) {
			suggestRequestBuilder.setIndices(toArray(suggestQuery.getSearchIndices()));
		}
		
		if(isNotEmpty(suggestQuery.getSuggestions())){
			for(SuggestionBuilder<?> suggestionBuilder : suggestQuery.getSuggestions()){
				suggestRequestBuilder.addSuggestion(suggestionBuilder);
			}
		}
		
		return suggestRequestBuilder.execute().actionGet();
	}

	private List<String> extractIds(SearchResponse response) {
		List<String> ids = new ArrayList<String>();
		for (SearchHit hit : response.getHits()) {
			if (hit != null) {
				ids.add(hit.getId());
			}
		}
		return ids;
	}

	/**
	 * @param query
	 * @param clazz
	 * @return
	 */
	private <T> SearchRequestBuilder prepareSearch(Query query, Class<T> clazz) {
		if (query.getIndices().isEmpty()) {
			query.addIndices(getIndexName(clazz));
		}
		if (query.getTypes().isEmpty()) {
			query.addTypes(getType(clazz));
		}
		return prepareSearch(query);
	}

	private IndexRequestBuilder prepareIndex(IndexQuery query) {
		try {
			String indexName = isBlank(query.getIndexName()) ? getIndexName(query.getObject().getClass()) : query.getIndexName();
			String type = isBlank(query.getType()) ? getType(query.getObject().getClass()) : query.getType();

			IndexRequestBuilder indexRequestBuilder = es.prepareIndex(indexName, type, query.getId()).setSource(JsonHelper.toJsonString(query.getObject()));

			if (query.getVersion() != null) {
				indexRequestBuilder.setVersion(query.getVersion());
				indexRequestBuilder.setVersionType(EXTERNAL);
			}
			return indexRequestBuilder;
		} catch (Exception e) {
			throw new ElasticsearchException("failed to index the document [id: " + query.getId() + "]", e);
		}
	}

	private SearchRequestBuilder prepareSearch(Query query) {
		Assert.notNull(query.getIndices(), "No index defined for Query");
		Assert.notNull(query.getTypes(), "No type defined for Query");

		int startRecord = 0;
		SearchRequestBuilder searchRequestBuilder = es.prepareSearch(toArray(query.getIndices())).setSearchType(DFS_QUERY_THEN_FETCH).setTypes(toArray(query.getTypes()));

		if (query.getPageable() != null) {
			startRecord = (query.getPageable().getCurrentPage() - 1) * query.getPageable().getPageSize();
			searchRequestBuilder.setSize(query.getPageable().getPageSize());
		}
		searchRequestBuilder.setFrom(startRecord);

		if (!query.getFields().isEmpty()) {
			searchRequestBuilder.addFields(toArray(query.getFields()));
		}

		if (query.getPageable().getSorts() != null) {
			for (String order : query.getPageable().getSorts().keySet()) {
				searchRequestBuilder.addSort(order, StringUtils.equalsIgnoreCase("desc", query.getPageable().getSorts().get(order).sort()) ? SortOrder.DESC : SortOrder.ASC);
			}
		}
		return searchRequestBuilder;
	}

	private <T> EsPage<T> parseResult(SearchResponse response, final Class<T> clazz, Page<?> page,HighlightFieldResultParser<T> parser) {
		EsPage<T> facetPage = new EsPage<T>();
		SearchHits hits = response.getHits();
		long totalHits = hits.totalHits();
		facetPage.setTotalCount((int) totalHits);
		List<T> results = new ArrayList<T>();
		for (SearchHit hit : hits) {
			if (hit != null) {
				T entity = JsonHelper.fromJsonString(hit.sourceAsString(), clazz);
				if(null != parser){
					parser.parseResult(hit.getHighlightFields(), entity);
				}else{
					handleHighlightFields(hit.getHighlightFields(), entity);
				}
				results.add(entity);
			}
		}
		facetPage.setResult(results);
		facetPage.setFacets(parseFacet(response.getFacets()));
		if (null != page) {
			facetPage.setPageSize(page.getPageSize()).setCurrentPage(page.getCurrentPage());
		}
		return facetPage;
	}
	
	private Map<String, Map<String, Integer>> parseFacet(Facets facets){
		Map<String, Map<String, Integer>> facetMap = new HashMap<String, Map<String, Integer>>();
		if(null != facets){
			Map<String,Facet> facetsMap = facets.facetsAsMap();
			for (String key : facetsMap.keySet()) {
				TermsFacet f = (TermsFacet) facetsMap.get(key);
				Map<String, Integer> terms = new HashMap<String, Integer>();
				for (TermsFacet.Entry entry : f) {
					terms.put(entry.getTerm().string(), entry.getCount());
				}
				facetMap.put(key, terms);
			}
		}
		return facetMap;
	}

	/**
	 * 设置高亮字段
	 * 
	 * @param hit
	 * @param entity
	 * @return
	 */
	private <T> Object handleHighlightFields(Map<String, HighlightField> highlightFields, Object entity) {
		for (String key : highlightFields.keySet()) {
			String value = highlightFields.get(key).fragments()[0].string();
			GodHands.setFieldValue(entity, key, value);
		}
		return entity;
	}

	/**
	 * @param searchRequest
	 * @param searchQuery
	 * @return
	 */
	private SearchResponse doSearch(SearchRequestBuilder searchRequest, SearchQuery searchQuery) {
		if (searchQuery.getFilter() != null) {
			searchRequest.setFilter(searchQuery.getFilter());
		}

		if (searchQuery.getElasticsearchSort() != null) {
			for (SortBuilder sort : searchQuery.getElasticsearchSort()) {
				searchRequest.addSort(sort);
			}
		}

		if (CollectionUtils.isNotEmpty(searchQuery.getFacets())) {
			for (FacetBuilder facet : searchQuery.getFacets()) {
				if (applyQueryFilter() && searchQuery.getFilter() != null) {
					facet.facetFilter(searchQuery.getFilter());
				}
				searchRequest.addFacet(facet);
			}
		}

		if (searchQuery.getHighlightFields() != null) {
			for (HighlightBuilder.Field highlightField : searchQuery.getHighlightFields()) {
				searchRequest.addHighlightedField(highlightField);
			}
		}

		QueryBuilder query = searchQuery.getQuery();
		return searchRequest.setQuery(query).execute().actionGet();
	}

	/**
	 * @param clazz
	 * @return
	 */
	@Override
	public String getIndexName(Class<?> clazz) {

		Document document = GodHands.getAccessibleAnnotation(clazz, Document.class);
		if (null == document) {
			return clazz.getSimpleName().toLowerCase();
		}
		return document.index();
	}
	
	/**
	 * @param clazz
	 * @return
	 */
	@Override
	public String getType(Class<?> clazz) {
		Document document = GodHands.getAccessibleAnnotation(clazz, Document.class);
		if (null == document) {
			return clazz.getSimpleName().toLowerCase();
		}
		return document.type();
	}

	/**
	 * @param clazz
	 * @return
	 */
	private <T> boolean createIndexIfNotCreated(Class<T> clazz) {
		return indexExists(getIndexName(clazz)) || createIndexWithSettings(clazz);
	}

	/**
	 * @param indexName
	 * @return
	 */
	private boolean indexExists(String indexName) {
		return es.admin().indices().exists(indicesExistsRequest(indexName)).actionGet().isExists();
	}

	/**
	 * @param clazz
	 * @return
	 */
	private <T> boolean createIndexWithSettings(Class<T> clazz) {
		return es.admin().indices().create(Requests.createIndexRequest(getIndexName(clazz)).settings(getSettings(clazz))).actionGet().isAcknowledged();
	}

	private Map<String, String> getSettings(Class<?> clazz) {
		MapBuilder<String, String> settings = new MapBuilder<String, String>();
		Document document = GodHands.getAccessibleAnnotation(clazz, Document.class);
		if (null != document) {
			settings.put("index.number_of_shards", String.valueOf(document.shards())).put("index.number_of_replicas", String.valueOf(document.replicas()))
					.put("index.refresh_interval", document.refreshInterval()).put("index.store.type", document.indexStoreType());
		}
		return settings.map();
	}

	/**
	 * @return
	 */
	protected boolean applyQueryFilter() {
		return true;
	}

	private static String[] toArray(List<String> values) {
		String[] valuesAsArray = new String[values.size()];
		return values.toArray(valuesAsArray);

	}

	@Override
	public Client getEsClient() {
		return es;
	}

}
