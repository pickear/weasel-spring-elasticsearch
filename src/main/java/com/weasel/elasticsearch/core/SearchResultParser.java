package com.weasel.elasticsearch.core;

import org.elasticsearch.action.search.SearchResponse;

import com.weasel.lang.EsPage;

/**
 * 搜索结果的解析器(转换结果)
 * @author Dylan
 * @time 2013年12月29日
 */
public interface SearchResultParser<T> {
	
	/**
	 * @param response
	 * @param clazz
	 * @param page
	 * @return
	 */
	EsPage<T> parseResult(SearchResponse response, final Class<T> clazz, EsPage<T> page);

}
