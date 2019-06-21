package org.cloudbus.cloudsim.examples;

public class BandwidthInfo {

	double bandwidth;
	int sourceNodeId;
	int destinationNodeId;
	double transferTime; // transferTime=partitionSize/bandwidth
	String partitionName;

	public BandwidthInfo(int sourceNodeId, int destinationNodeId, double bandwidth) {

		this.bandwidth = bandwidth;
		this.sourceNodeId = sourceNodeId;
		this.destinationNodeId = destinationNodeId;

	}

	public BandwidthInfo(int sourceNodeId, int destinationNodeId, double transferTime,
			String partitionName) {

		this.sourceNodeId = sourceNodeId;
		this.destinationNodeId = destinationNodeId;
		this.transferTime = transferTime;
		this.partitionName = partitionName;
	}
	
	

	public double getBandwidth() {
		return bandwidth;
	}

	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
	}

	public int getSourceNodeId() {
		return sourceNodeId;
	}

	public void setSourceNodeId(int sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
	}

	public int getDestinationNodeId() {
		return destinationNodeId;
	}

	public void setDestinationNodeId(int destinationNodeId) {
		this.destinationNodeId = destinationNodeId;
	}

	public double getTransferTime() {
		return transferTime;
	}

	public void setTransferTime(double transferTime) {
		this.transferTime = transferTime;
	}

	public String getPartitionName() {
		return partitionName;
	}

	public void setPartitionName(String partitionName) {
		this.partitionName = partitionName;
	}

}
