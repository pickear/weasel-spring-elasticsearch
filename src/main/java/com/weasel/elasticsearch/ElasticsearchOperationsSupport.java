package com.weasel.elasticsearch;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static com.weasel.elasticsearch.core.query.Query.DEFAULT_PAGE;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import com.weasel.core.EsPage;
import com.weasel.core.Page;
import com.weasel.core.annotation.Id;
import com.weasel.core.helper.DemonPredict;
import com.weasel.core.helper.GodHands;
import com.weasel.core.helper.JsonHelper;
import com.weasel.elasticsearch.core.ElasticsearchRepository;
import com.weasel.elasticsearch.core.HighlightFieldResultParser;
import com.weasel.elasticsearch.core.SearchResultParser;
import com.weasel.elasticsearch.core.query.DeleteQuery;
import com.weasel.elasticsearch.core.query.GetQuery;
import com.weasel.elasticsearch.core.query.IndexQuery;
import com.weasel.elasticsearch.core.query.MoreLikeThisQuery;
import com.weasel.elasticsearch.core.query.NativeSearchQueryBuilder;
import com.weasel.elasticsearch.core.query.SearchQuery;
import com.weasel.elasticsearch.core.query.SuggestQuery;
import com.weasel.elasticsearch.core.query.UpdateQuery;
import com.weasel.elasticsearch.core.query.UpdateQueryBuilder;

/**
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Ryan Henszey
 * @author Dylan
 * @time 2013-11-18
 */
public class ElasticsearchOperationsSupport <ID extends Serializable,T>{

	protected ElasticsearchRepository repository;
	protected Class<T> entityClass;
	protected String idName;
	
	@SuppressWarnings("unchecked")
	public ElasticsearchOperationsSupport(){
		entityClass = (Class<T>) GodHands.genericsTypes(getClass())[1];
		Field [] fields = GodHands.getAccessibleFields(entityClass);
		for(Field field : fields){
			if(field.isAnnotationPresent(Id.class) || StringUtils.equalsIgnoreCase("id", field.getName())) {  
                this.idName = field.getName();  
            }  
		}
	}

	@Autowired
	public void setRepository(ElasticsearchRepository repository) {
		this.repository = repository;
		createIndex();
	}
	
	private void createIndex(){
		repository.createIndex(entityClass);
	}
	
	public T findOne(ID id) {
		GetQuery query = new GetQuery();
		query.setId(stringIdRepresentation(id));
		return repository.queryForObject(query, entityClass);
	}
	
	public Iterable<T> findAll() {
		int itemCount = (int) this.count();
		if (itemCount == 0) {
			return Collections.<T> emptyList();
		}
		SearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		return repository.queryForList(query, entityClass);
	}
	
	public Page<T> findAll(Page<T> page) {
		SearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).withPageable(page).build();
		return repository.queryForPage(query, entityClass);
	}
	
	public long count() {
		SearchQuery query = new NativeSearchQueryBuilder().withQuery(matchAllQuery()).build();
		return repository.count(query, entityClass);
	}
	
	public <S extends T> S save(S entity) {
		DemonPredict.notNull(entity, "Cannot save 'null' entity.");
		repository.index(createIndexQuery(entity));
		repository.refresh(repository.getIndexName(entityClass), true);
		return entity;
	}
	
	public <S extends T> void update(S entity,boolean upsert) {
		 IndexRequest indexRequest = new IndexRequest();
		 indexRequest.source(JsonHelper.toJsonString(entity));
		 UpdateQuery updateQuery = new UpdateQueryBuilder().withId(stringIdRepresentation(extractIdFromBean(entity))).withDoUpsert(upsert)
	                .withClass(entity.getClass()).withIndexRequest(indexRequest).build();
		 repository.update(updateQuery);
	}
	
	public <S extends T> List<S> save(List<S> entities) {
		DemonPredict.notNull(entities, "Cannot insert 'null' as a List.");
		List<IndexQuery> queries = new ArrayList<IndexQuery>();
		for (S s : entities) {
			queries.add(createIndexQuery(s));
		}
		repository.bulkIndex(queries);
		repository.refresh(repository.getIndexName(entityClass), true);
		return entities;
	}

	public <S extends T> S index(S entity) {
		return save(entity);
	}

	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		DemonPredict.notNull(entities, "Cannot insert 'null' as a List.");
		if (!(entities instanceof Collection<?>)) {
			throw new RuntimeException("Entities have to be inside a collection");
		}
		List<IndexQuery> queries = new ArrayList<IndexQuery>();
		for (S s : entities) {
			queries.add(createIndexQuery(s));
		}
		repository.bulkIndex(queries);
		repository.refresh(repository.getIndexName(entityClass), true);
		return entities;
	}
	
	public Iterable<T> search(QueryBuilder query) {
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(query).build();
		int count = (int) repository.count(searchQuery, entityClass);
		if (count == 0) {
			return Collections.<T> emptyList();
		}
		Page<T> page = new Page<T>();
		page.setPageSize(count);
		searchQuery.setPageable(page);
		return repository.queryForPage(searchQuery, entityClass).getResult();
	}

	public Page<T> search(QueryBuilder query, Page<T> page) {
		SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(query).withPageable(page).build();
		return repository.queryForPage(searchQuery, entityClass);
	}

	public EsPage<T> search(SearchQuery query) {
		return repository.queryForPage(query, entityClass);
	}
	
	public EsPage<T> search(SearchQuery query,SearchResultParser<T> parser){
		
		return repository.queryForPage(query, entityClass,parser);
	}
	
	public EsPage<T> search(SearchQuery query,HighlightFieldResultParser<T> parser){
		
		return repository.queryForPage(query, entityClass,parser);
	}
	
	public Page<T> searchSimilar(T entity, SearchQuery searchQuery) {
		DemonPredict.notNull(entity, "Cannot search similar records for 'null'.");
		DemonPredict.notNull(searchQuery.getFields(), "Fields cannot be 'null'");
		MoreLikeThisQuery query = new MoreLikeThisQuery();
		query.setId(stringIdRepresentation(extractIdFromBean(entity)));
		query.setPageable(searchQuery.getPageable() != null ? searchQuery.getPageable() : DEFAULT_PAGE);
		query.addFields(searchQuery.getFields().toArray(new String[searchQuery.getFields().size()]));
		if (!searchQuery.getIndices().isEmpty()) {
			query.addSearchIndices(searchQuery.getIndices().toArray(new String[searchQuery.getIndices().size()]));
		}
		if (!searchQuery.getTypes().isEmpty()) {
			query.addSearchTypes(searchQuery.getTypes().toArray(new String[searchQuery.getTypes().size()]));
		}
		return repository.moreLikeThis(query, entityClass);
	}
	
	public List<String> suggest(String field,String keyWord,int returnSize){
		SuggestQuery query = new SuggestQuery().addSuggestion(SuggestBuilder.phraseSuggestion(repository.getIndexName(entityClass)).field(field).text(keyWord).size(returnSize));
		SuggestResponse response = suggest(query);
		List<String> suggests = new ArrayList<String>();
		for(Entry<? extends Option> e:response.getSuggest().getSuggestion(repository.getIndexName(entityClass)).getEntries()){
			for(Option o:e.getOptions()){
				suggests.add(o.getText().string());
			}
		}
		return suggests;
	}
	
	public SuggestResponse suggest(SuggestQuery suggestQuery) {
		return repository.suggest(suggestQuery, entityClass);
	}

	public void delete(ID id) {
		DemonPredict.notNull(id, "Cannot delete entity with id 'null'.");
		repository.delete(repository.getIndexName(entityClass), repository.getType(entityClass),
				stringIdRepresentation(id));
		repository.refresh(repository.getIndexName(entityClass), true);
	}

	public void delete(T entity) {
		DemonPredict.notNull(entity, "Cannot delete 'null' entity.");
		delete(extractIdFromBean(entity));
		repository.refresh(repository.getIndexName(entityClass), true);
	}

	public void delete(Iterable<? extends T> entities) {
		DemonPredict.notNull(entities, "Cannot delete 'null' list.");
		for (T entity : entities) {
			delete(entity);
		}
	}

	public void deleteAll() {
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(matchAllQuery());
		repository.delete(deleteQuery, entityClass);
		repository.refresh(repository.getIndexName(entityClass), true);
	}
	
	
	public Client getEsClient() {
		return repository.getEsClient();
	}

	private IndexQuery createIndexQuery(T entity) {
		IndexQuery query = new IndexQuery();
		query.setObject(entity);
		query.setId(stringIdRepresentation(extractIdFromBean(entity)));
	//	query.setVersion(extractVersionFromBean(entity));
		return query;
	}
	
	public boolean exists(ID id) {
		return findOne(id) != null;
	}
	
	@SuppressWarnings("unchecked")
	public ID extractIdFromBean(T entity) {
		return (ID) GodHands.getFieldValue(entity, idName);
	}
	
	
	/**
	 * 将id转化为String类型，请确保该id唯一和返回非null
	 * @param id
	 * @return
	 */
	protected String stringIdRepresentation(ID id){
		if(null == id)
			return null;
		return String.valueOf(id);
	}
	
	
}
