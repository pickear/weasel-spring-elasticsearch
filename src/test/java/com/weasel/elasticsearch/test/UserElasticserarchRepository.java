package com.weasel.elasticsearch.test;

import com.weasel.elasticsearch.ElasticsearchRepositorySupport;
import com.weasel.elasticsearch.test.domain.User;

/**
 * @author Dylan
 * @time 2013-11-18
 */
public interface UserElasticserarchRepository extends ElasticsearchRepositorySupport<Integer, User> {

}
