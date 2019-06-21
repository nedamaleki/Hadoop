package org.cloudbus.cloudsim.examples;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;

public class CloudletWithOutputFile extends Cloudlet{

	
	//this is new property of cloudlet that should be initialized
	public List<KeyValuePair> mapOutputDataList;
	private long myCloudletFileSize;
	private long myCloudletOutputSize;
	
	CloudletWithOutputFile (int cloudletId, long cloudletLength, int pesNumber, long cloudletFileSize, long cloudletOutputSize, UtilizationModel utilizationModelCpu, UtilizationModel utilizationModelRam, UtilizationModel utilizationModelBw, boolean record, double finishingTime) {
		 super(cloudletId, cloudletLength, pesNumber, cloudletFileSize, cloudletOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw, record, finishingTime);
		 mapOutputDataList=new ArrayList<KeyValuePair>();
	}
	
	public long getMyCloudletFileSize() {
		return myCloudletFileSize;
	}

	public void setMyCloudletFileSize(long myCloudletFileSize) {
		this.myCloudletFileSize = myCloudletFileSize;
	}

	public long getMyCloudletOutputSize() {
		return myCloudletOutputSize;
	}

	public void setMyCloudletOutputSize(long myCloudletOutputSize) {
		this.myCloudletOutputSize = myCloudletOutputSize;
	}

	
	public List<KeyValuePair> getMapOutputDataList() {
		return mapOutputDataList;
	}

	public void setMapOutputDataList(List<KeyValuePair> mapOutputDataList) {
		this.mapOutputDataList = mapOutputDataList;
	}

	public void AddToMapOutputDataList (KeyValuePair kv) {
		
		this.mapOutputDataList.add(kv);
		this.myCloudletOutputSize +=kv.value;
	}
	
}

