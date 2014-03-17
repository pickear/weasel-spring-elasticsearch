package com.weasel.elasticsearch;

import java.io.Serializable;
import java.util.List;

import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;

import com.weasel.core.EsPage;
import com.weasel.core.Page;
import com.weasel.elasticsearch.core.HighlightFieldResultParser;
import com.weasel.elasticsearch.core.SearchResultParser;
import com.weasel.elasticsearch.core.query.SearchQuery;
import com.weasel.elasticsearch.core.query.SuggestQuery;

/**
 * @author Dylan
 * @time 2013-11-18
 */
public interface ElasticsearchRepositorySupport<ID extends Serializable,T> {

	/**
	 * @param id
	 * @return
	 */
	T findOne(ID id);

	/**
	 * @return
	 */
	long count();

	/**
	 * @return
	 */
	Iterable<T> findAll();

	/**
	 * @param page
	 * @return
	 */
	Page<T> findAll(Page<T> page);

	/**
	 * @param entities
	 * @return
	 */
	<S extends T> Iterable<S> save(Iterable<S> entities);

	/**
	 * @param entity
	 * @return
	 */
	<S extends T> S index(S entity);

	/**
	 * @param entity
	 * @return
	 */
	<S extends T> S save(S entity);
	
	/**
	 * @param entity
	 * @param upsert 不存在是否插入
	 */
	<S extends T> void update(S entity,boolean upsert);

	/**
	 * @param id
	 * @return
	 */
	boolean exists(ID id);

	/**
	 * @param query
	 * @return
	 */
	Iterable<T> search(QueryBuilder query);

	/**
	 * @param query
	 * @param page
	 * @return
	 */
	Page<T> search(QueryBuilder query, Page<T> page);

	/**
	 * @param query
	 * @return
	 */
	EsPage<T> search(SearchQuery query);
	
	/**
	 * @param query
	 * @param parser
	 * @return
	 */
	EsPage<T> search(SearchQuery query,SearchResultParser<T> parser);
	
	/**
	 * @param query
	 * @param parser
	 * @return
	 */
	EsPage<T> search(SearchQuery query,HighlightFieldResultParser<T> parser);

	/**
	 * @param entity
	 * @param searchQuery
	 * @return
	 */
	Page<T> searchSimilar(T entity, SearchQuery searchQuery);

	/**
	 * @param id
	 */
	void delete(ID id);

	/**
	 * @param entity
	 */
	void delete(T entity);

	/**
	 * @param entities
	 */
	void delete(Iterable<? extends T> entities);

	/**
	 * 
	 */
	void deleteAll();

	/**
	 * @param entities
	 * @return
	 */
	<S extends T> List<S> save(List<S> entities);
	
	/**
	 * @param suggestQuery
	 * @param clazz
	 * @return
	 */
	SuggestResponse suggest(SuggestQuery suggestQuery);

	/**
	 * @param keyWord
	 * @param returnSize
	 * @return
	 */
	List<String> suggest(String field,String keyWord, int returnSize);
	
	/**
	 * @return
	 */
	Client getEsClient();
	

}
