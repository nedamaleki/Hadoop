package org.cloudbus.cloudsim.examples;

public class KeyValuePair {
	
	String key;
	int value;
	int numberOfReducers;
	
	
	


KeyValuePair(String key, int value){
		
		this.key=key;
		this.value=value;
		
		
	}
	
KeyValuePair(String key, int value, int numberOfReducers){
		
		this.key=key;
		this.value=value;
		this.numberOfReducers=(int) Math.ceil(value/1000)+1;
		
	}
	

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	
	public int getNumberOfReducers() {
		return numberOfReducers;
	}


	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}
	
	
}


