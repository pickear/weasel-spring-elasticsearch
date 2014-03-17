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
package com.weasel.elasticsearch.core.query;

import static org.apache.commons.collections.CollectionUtils.addAll;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

import com.weasel.lang.Page;

/**
 * AbstractQuery
 * 
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Dylan
 */
abstract class AbstractQuery implements Query {

	protected Page<?> pageable = DEFAULT_PAGE;
	protected List<String> indices = new ArrayList<String>();
	protected List<String> types = new ArrayList<String>();
	protected List<String> fields = new ArrayList<String>();

	
	@Override
	public Page<?> getPageable() {
		return this.pageable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <T extends Query> T setPageable(Page<?> pageable) {
		Assert.notNull(pageable);
		this.pageable = pageable;
		return (T) this;
	}

	@Override
	public void addFields(String... fields) {
		addAll(this.fields, fields);
	}

	@Override
	public List<String> getFields() {
		return fields;
	}

	@Override
	public List<String> getIndices() {
		return indices;
	}

	@Override
	public void addIndices(String... indices) {
		addAll(this.indices, indices);
	}

	@Override
	public void addTypes(String... types) {
		addAll(this.types, types);
	}

	@Override
	public List<String> getTypes() {
		return types;
	}
}
