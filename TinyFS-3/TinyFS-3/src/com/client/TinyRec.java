package com.client;

public class TinyRec {
	private byte[] payload = null;
	private RID ID = null;
	
	public byte[] getPayload() {
		return payload;
	}
	public void setPayload(byte[] p) {
		this.payload = p;
	}
	
	public RID getRID() {
		return ID;
	}
	public void setRID(RID inputID) {
		ID = inputID;
	}
}
