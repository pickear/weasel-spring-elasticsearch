package com.weasel.elasticsearch.test.domain;

import com.weasel.lang.BaseObject;

/**
 * @author Dylan
 * @time 2014年2月24日
 */
public class Address extends BaseObject<Integer> {

	/**
	 * Adress.java
	 */
	private static final long serialVersionUID = 2628424508450827265L;
	
	private String province;
	
	private String city;

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	
	

}
