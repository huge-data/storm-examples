package org.misi.tbda.dataStreams.dominio;

public class Topic {
    private String hashtag;
    private String frecuency;
	
    public Topic() {
	
	}
    public Topic(String hashtag, String frecuency) {
		super();
		this.hashtag = hashtag;
		this.frecuency = frecuency;
	}
	public String getHashtag() {
		return hashtag;
	}
	public void setHashtag(String hashtag) {
		this.hashtag = hashtag;
	}
	public String getFrecuency() {
		return frecuency;
	}
	public void setFrecuency(String frecuency) {
		this.frecuency = frecuency;
	}
    
}
