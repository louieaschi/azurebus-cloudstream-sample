package com.brillio.azurebuscloudstreamsample;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SampleObject {
	@JsonProperty("name")
	String name;
	@JsonProperty("id")
	Integer Id;
	public String getName() {
		// TODO Auto-generated method stub
		return name;
	}
	public void setName(String string) {
		name = string;
		
	}
}
