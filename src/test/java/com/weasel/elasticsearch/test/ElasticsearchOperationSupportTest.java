package com.weasel.elasticsearch.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.weasel.core.EsPage;
import com.weasel.core.Page;
import com.weasel.core.helper.DemonPredict;
import com.weasel.core.helper.JsonHelper;
import com.weasel.elasticsearch.core.HighlightFieldResultParser;
import com.weasel.elasticsearch.core.SearchResultParser;
import com.weasel.elasticsearch.core.query.NativeSearchQueryBuilder;
import com.weasel.elasticsearch.core.query.SearchQuery;
import com.weasel.elasticsearch.test.domain.Address;
import com.weasel.elasticsearch.test.domain.User;

/**
 * @author Dylan
 * @time 2013-11-18
 */
public class ElasticsearchOperationSupportTest extends AbstractESTest{

	@Autowired
	private UserElasticserarchRepository repository;
	
	@Test
	public void createIndex(){
		DemonPredict.notNull(repository);
	}
	
	@Test
	public void save(){
		repository.deleteAll();
		User user = new User();
		user.setId(1);
		user.setPassword("123");
		user.setUsername("张三");
		repository.save(user);
	}
	
	@Test
	public void saveList(){
		repository.deleteAll();
		List<User> users = new ArrayList<User>();
		int saveSize = 20;
		for(int i = 0;i < saveSize;i++){
			User user = new User();
			user.setId(i);
			user.setPassword("p"+i);
			user.setUsername("u"+i);
			users.add(user);
		}
		repository.save(users);
		DemonPredict.isTrue(repository.count() == saveSize);
	}
	
	@Test
	public void findOne(){
		repository.deleteAll();
		User user = new User();
		user.setId(1);
		user.setPassword("p"+1);
		user.setUsername("u"+1);
		repository.save(user);
		DemonPredict.isTrue(repository.count() == 1);
		user = repository.findOne(user.getId());
		DemonPredict.notNull(user);
	}
	
	@Test
	public void findAll(){
		saveList();
		Iterable<User> users = repository.findAll();
		Iterator<User> us =  users.iterator();
		int findSize = 0;
		for(;us.hasNext();){
			findSize+=1;
			us.next();
		}
		DemonPredict.isTrue(findSize == 20);
	}
	
	@Test
	public void deleteOne(){
		saveList();
		repository.delete(1);
		DemonPredict.isTrue(repository.count()==19);
	}
	@Test
	public void delete(){
		saveList();
		User user = new User();
		user.setId(1);
		repository.delete(user);
		user = repository.findOne(user.getId());
		DemonPredict.isTrue(null == user);
	}
	
	@Test
	public void search1(){
		Page<User> page = new Page<User>();
		page.setCurrentPage(2);
		page.setPageSize(10);
		BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
		PrefixQueryBuilder fieldQueryBuilder = QueryBuilders.prefixQuery("username", "u");
		booleanQuery.should(fieldQueryBuilder);
		page = repository.search(booleanQuery, page);
		if(null != page){
			for(User user : page.getResult()){
				System.out.println(user.getUsername());
			}
		}
	}
	
	@Test
	public void search2(){
		Page<User> page = new Page<User>();
		page.setPageSize(20);
	//	page.addSorts("id", Page.Sort.DESC);
		
		BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
		PrefixQueryBuilder fieldQueryBuilder = QueryBuilders.prefixQuery("username", "u");
		booleanQuery.should(fieldQueryBuilder);
		
		TermsFacetBuilder facet =  FacetBuilders.termsFacet("uu").field("username").size(Integer.MAX_VALUE);
		
		TermsFilterBuilder filter = FilterBuilders.termsFilter("username", "u2","u3","u4");
		
		Field field = new HighlightBuilder.Field("username").preTags("<font color='red'>").postTags("</font>");
	
		SearchQuery  query = new NativeSearchQueryBuilder().withQuery(booleanQuery)
														   .withFacet(facet)
														   .withFilter(filter)
														   .withHighlightFields(field)
														   .withSort(SortBuilders.fieldSort("id").order(SortOrder.DESC))
														   .withPageable(page)
														   .build();
		
		EsPage<User> facetPage = repository.search(query);
		for(User user : facetPage.getResult()){
			System.out.println(user.getUsername());
		}
		Map<String, Map<String, Integer>> facetsMap = facetPage.getFacets();
		for(String key : facetsMap.keySet()){
			System.out.println(key);
			for(String key2 : facetsMap.get(key).keySet()){
				System.out.println(key2 + " - " + facetsMap.get(key).get(key2));
			}
		}
	}
	
	@Test
	public void search3(){
		Page<User> page = new Page<User>();
		page.setPageSize(20);
	//	page.addSorts("id", Page.Sort.DESC);
		
		BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
		PrefixQueryBuilder fieldQueryBuilder = QueryBuilders.prefixQuery("username", "u");
		booleanQuery.should(fieldQueryBuilder);
		
		TermsFacetBuilder facet =  FacetBuilders.termsFacet("uu").field("username").size(Integer.MAX_VALUE);
		
		TermsFilterBuilder filter = FilterBuilders.termsFilter("username", "u2","u3","u4");
		
		Field field = new HighlightBuilder.Field("username").preTags("<font color='red'>").postTags("</font>");
		Field field2 = new HighlightBuilder.Field("address.province").preTags("<font color='red'>").postTags("</font>");
	
		SearchQuery  query = new NativeSearchQueryBuilder().withQuery(booleanQuery)
														   .withFacet(facet)
														   .withFilter(filter)
														   .withHighlightFields(field,field2)
														   .withSort(SortBuilders.fieldSort("id").order(SortOrder.DESC))
														   .withPageable(page)
														   .build();
		
		EsPage<User> facetPage = repository.search(query,new HighlightFieldResultParser<User>() {
			
			@Override
			public void parseResult(Map<String, HighlightField> highlightFields,User entity) {
				entity.setUsername(highlightFields.get("username").getFragments()[0].toString());
				Address address = entity.getAddress();
				if(null != address){
					address.setProvince(highlightFields.get("address.province").getFragments()[0].toString());
				}
			}
		});
		
		for(User user : facetPage.getResult()){
			System.out.println(user.getUsername());
		}
		Map<String, Map<String, Integer>> facetsMap = facetPage.getFacets();
		for(String key : facetsMap.keySet()){
			System.out.println(key);
			for(String key2 : facetsMap.get(key).keySet()){
				System.out.println(key2 + " - " + facetsMap.get(key).get(key2));
			}
		}
	}
	
	@Test
	public void search4(){
		Page<User> page = new Page<User>();
		page.setPageSize(20);
	//	page.addSorts("id", Page.Sort.DESC);
		
		BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
		PrefixQueryBuilder fieldQueryBuilder = QueryBuilders.prefixQuery("username", "u");
		booleanQuery.should(fieldQueryBuilder);
		
		TermsFacetBuilder facet =  FacetBuilders.termsFacet("uu").field("username").size(Integer.MAX_VALUE);
		
		TermsFilterBuilder filter = FilterBuilders.termsFilter("username", "u2","u3","u4");
		
		Field field = new HighlightBuilder.Field("username").preTags("<font color='red'>").postTags("</font>");
	
		SearchQuery  query = new NativeSearchQueryBuilder().withQuery(booleanQuery)
														   .withFacet(facet)
														   .withFilter(filter)
														   .withHighlightFields(field)
														   .withSort(SortBuilders.fieldSort("id").order(SortOrder.DESC))
														   .withPageable(page)
														   .build();
		
		EsPage<User> facetPage = repository.search(query,new SearchResultParser<User>() {
			
			@Override
			public EsPage<User> parseResult(SearchResponse response, Class<User> clazz,EsPage<User> page) {
				
				SearchHits hits = response.getHits();
				long totalHits = hits.totalHits();
				page.setTotalCount((int) totalHits);
				List<User> results = new ArrayList<User>();
				for (SearchHit hit : hits) {
					if (hit != null) {
						User entity = JsonHelper.fromJsonString(hit.sourceAsString(), clazz);
					    handleHighlightFields(hit.getHighlightFields(), entity);
						results.add(entity);
					}
				}
				page.setResult(results);
				page.setFacets(parseFacet(response.getFacets()));
				if (null != page) {
					page.setPageSize(page.getPageSize()).setCurrentPage(page.getCurrentPage());
				}
				return page;
			}
		});
		
		for(User user : facetPage.getResult()){
			System.out.println(user.getUsername());
		}
		Map<String, Map<String, Integer>> facetsMap = facetPage.getFacets();
		for(String key : facetsMap.keySet()){
			System.out.println(key);
			for(String key2 : facetsMap.get(key).keySet()){
				System.out.println(key2 + " - " + facetsMap.get(key).get(key2));
			}
		}
	}
	
	private User handleHighlightFields(Map<String, HighlightField> highlightFields, User user) {
		user.setUsername(highlightFields.get("username").fragments()[0].string());
		return user;
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
	
	@Test
	public void suggest(){
		List<String> suggestions = repository.suggest("username","u2", 10);
		for(String suggetion : suggestions){
			System.out.println(suggetion);
		}
	}
}
