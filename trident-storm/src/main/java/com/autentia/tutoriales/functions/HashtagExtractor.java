package com.autentia.tutoriales.functions;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.tuple.Values;

public class HashtagExtractor extends BaseFunction {

	private static final long serialVersionUID = -3408773948564338830L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		final Status status = (Status) tuple.get(0);

		for (HashtagEntity hashtag : status.getHashtagEntities()) {
			collector.emit(new Values(hashtag.getText()));
		}
	}

}
