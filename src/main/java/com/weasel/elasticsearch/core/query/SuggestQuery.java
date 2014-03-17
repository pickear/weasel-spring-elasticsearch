package com.weasel.elasticsearch.core.query;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;

/**
 * @author Dylan
 * @time 2013-11-19
 */
public class SuggestQuery {

	private String preference;
	private List<String> searchIndices = new ArrayList<String>();
	private List<String> routing = new ArrayList<String>();
	private String suggestText;
	private List<SuggestionBuilder<?>> suggestions = new ArrayList<SuggestionBuilder<?>>();

	public String getPreference() {
		return preference;
	}

	public SuggestQuery withPreference(String prefernce) {
		this.preference = prefernce;
		return this;
	}

	public List<String> getSearchIndices() {
		return searchIndices;
	}

	public SuggestQuery setSearchIndices(List<String> searchIndices) {
		this.searchIndices = searchIndices;
		return this;
	}
	
	public SuggestQuery addSearchIndices(String searchIndice){
		this.searchIndices.add(searchIndice);
		return this;
	}

	public List<String> getRouting() {
		return routing;
	}

	public SuggestQuery setRouting(List<String> routing) {
		this.routing = routing;
		return this;
	}
	
	public SuggestQuery addRouting(String routing){
		this.routing.add(routing);
		return this;
	}

	public String getSuggestText() {
		return suggestText;
	}
	
	public SuggestQuery withSuggestText(String suggestText){
		this.suggestText =suggestText;
		return this;
	}

	public List<SuggestionBuilder<?>> getSuggestions() {
		return suggestions;
	}

	public SuggestQuery setSuggestions(List<SuggestionBuilder<?>> suggestions) {
		this.suggestions = suggestions;
		return this;
	}
	
	public SuggestQuery addSuggestion(SuggestionBuilder<?> suggestion){
		this.suggestions.add(suggestion);
		return this;
	}


}
