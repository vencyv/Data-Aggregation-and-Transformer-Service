package com.dataaggregation.config;

import java.util.Date;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class DataAggregatorConfig extends TableServiceEntity {
	
	private Date Timestamp;
	private String Configuration;
	private String ScoreRetrieverContractMapping;
	private String Owner;
	private String TraumaModelContratcMapping;
	public Date getTimestamp() {
		return Timestamp;
	}
	public void setTimestamp(Date timestamp) {
		Timestamp = timestamp;
	}
	public String getConfiguration() {
		return Configuration;
	}
	public void setConfiguration(String configuration) {
		Configuration = configuration;
	}
	public String getOwner() {
		return Owner;
	}
	public void setOwner(String owner) {
		Owner = owner;
	}
	public String getScoreRetrieverContractMapping() {
		return ScoreRetrieverContractMapping;
	}
	public void setScoreRetrieverContractMapping(String scoreRetrieverContractMapping) {
		ScoreRetrieverContractMapping = scoreRetrieverContractMapping;
	}
	public String getTraumaModelContratcMapping() {
		return TraumaModelContratcMapping;
	}
	public void setTraumaModelContratcMapping(String traumaModelContratcMapping) {
		TraumaModelContratcMapping = traumaModelContratcMapping;
	}

}