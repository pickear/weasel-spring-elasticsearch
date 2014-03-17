package com.weasel.elasticsearch.core;

import java.util.List;

import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;

import com.weasel.core.EsPage;
import com.weasel.core.Page;
import com.weasel.elasticsearch.core.query.DeleteQuery;
import com.weasel.elasticsearch.core.query.GetQuery;
import com.weasel.elasticsearch.core.query.IndexQuery;
import com.weasel.elasticsearch.core.query.MoreLikeThisQuery;
import com.weasel.elasticsearch.core.query.SearchQuery;
import com.weasel.elasticsearch.core.query.SuggestQuery;
import com.weasel.elasticsearch.core.query.UpdateQuery;


/**
 * @author Dylan
 *
 */
public interface ElasticsearchRepository {

	/**
	 * Create an index for a class
	 * 
	 * @param clazz
	 * @param <T>
	 */
	<T> boolean createIndex(Class<T> clazz);

	/**
	 * Execute the query against elasticsearch and return the first returned object
	 * 
	 * @param query
	 * @param clazz
	 * @return the first matching object
	 */
	<T> T queryForObject(GetQuery query, Class<T> clazz);

	/**
	 * Execute the query against elasticsearch and return result as {@link Page}
	 * 
	 * @param query
	 * @param clazz
	 * @return
	 */
	<T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz);
	
	/**
	 * @param query
	 * @param clazz
	 * @param parser
	 * @return
	 */
	<T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz,SearchResultParser<T> parser);
	
	/**
	 * @param query
	 * @param clazz
	 * @param parser
	 * @return
	 */
	<T> EsPage<T> queryForPage(SearchQuery query, Class<T> clazz,HighlightFieldResultParser<T> parser);

    /**
     * Execute the search query against elasticsearch and return result as {@link List}
     *
     * @param query
     * @param clazz
     * @param <T>
     * @return
     */
    <T> List<T> queryForList(SearchQuery query, Class<T> clazz);

	/**
	 * Execute the query against elasticsearch and return ids
	 * 
	 * @param query
	 * @return
	 */
	<T> List<String> queryForIds(SearchQuery query);

	/**
	 * return number of elements found by for given query
	 * 
	 * @param query
	 * @param clazz
	 * @return
	 */
	<T> long count(SearchQuery query, Class<T> clazz);

	/**
	 * Index an object. Will do save or update
	 * 
	 * @param query
	 * @return returns the document id
	 */
	String index(IndexQuery query);

    /**
     * Partial update of the document
     *
     * @param updateQuery
     * @return
     */
    UpdateResponse update(UpdateQuery updateQuery);

	/**
	 * Bulk index all objects. Will do save or update
	 * 
	 * @param queries
	 */
	void bulkIndex(List<IndexQuery> queries);

	/**
	 * Delete the one object with provided id
	 * 
	 * @param indexName
	 * @param type
	 * @param id
	 * @return documentId of the document deleted
	 */
	String delete(String indexName, String type, String id);

	/**
	 * Delete the one object with provided id
	 * 
	 * @param clazz
	 * @param id
	 * @return documentId of the document deleted
	 */
	<T> String delete(Class<T> clazz, String id);

	/**
	 * Delete all records matching the query
	 * 
	 * @param clazz
	 * @param query
	 */
	<T> void delete(DeleteQuery query, Class<T> clazz);

	/**
     * Delete all records matching the query
     *
     * @param query
     */
    void delete(DeleteQuery query);

	/**
	 * Deletes an index for given entity
	 *
	 * @param clazz
	 * @param <T>
	 * @return
	 */
	<T> boolean deleteIndex(Class<T> clazz);

	/**
     * Deletes a type in an index
     *
     * @param index
     * @param type
     */
    void deleteType(String index, String type);

	/**
	 * check if index is exists
	 * 
	 * @param clazz
	 * @param <T>
	 * @return
	 */
	<T> boolean indexExists(Class<T> clazz);

    /**
     * check if type is exists in an index
     *
     * @param index
     * @param type
     * @return
     */
     boolean typeExists(String index, String type);

	/**
	 * refresh the index
	 * 
	 * @param indexName
	 * @param waitForOperation
	 */
	void refresh(String indexName, boolean waitForOperation);

	/**
	 * refresh the index
	 * 
	 * @param clazz
	 * @param waitForOperation
	 */
	<T> void refresh(Class<T> clazz, boolean waitForOperation);

	/**
	 * more like this query to search for documents that are "like" a specific document.
	 * 
	 * @param query
	 * @param clazz
	 * @param <T>
	 * @return
	 */
	<T> Page<T> moreLikeThis(MoreLikeThisQuery query, Class<T> clazz);

	/**
	 * @param clazz
	 * @return
	 */
	String getIndexName(Class<?> clazz);

	/**
	 * @param clazz
	 * @return
	 */
	String getType(Class<?> clazz);

	/**
	 * @param suggestQuery
	 * @param clazz
	 * @return
	 */
	<T> SuggestResponse suggest(SuggestQuery suggestQuery, Class<T> clazz);

	Client getEsClient();
	
}
