package com.weasel.elasticsearch.test.domain;

import com.weasel.core.BaseObject;
import com.weasel.core.annotation.Document;

/**
 * @author Dylan
 * @time 2013-11-18
 */
@Document(index="user",type="user")
public class User extends BaseObject<Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String username;
	
	private String password;
	
	private Address address;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}
	

}
