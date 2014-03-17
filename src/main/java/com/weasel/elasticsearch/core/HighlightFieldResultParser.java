package com.weasel.elasticsearch.core;

import java.util.Map;

import org.elasticsearch.search.highlight.HighlightField;

public interface HighlightFieldResultParser<T> {

	/**高亮字段解释器(转换器)
	 * @param highlightFields
	 * @param entity
	 */
	void parseResult(Map<String, HighlightField> highlightFields,T entity);
}
