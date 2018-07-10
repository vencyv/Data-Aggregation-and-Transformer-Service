package com.dataaggregation;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

import com.dataaggregation.service.DataAggregationService;

/**
 * @author ShaishavS Initializer class to initialize application as spring boot
 *         app
 */
@SpringBootApplication
@PropertySource(value={"classpath:constant.properties"})

public class AppInitializer {
	
	@Autowired
	DataAggregationService  dataAggregationService;
	
	public static void main(String[] args) {
		SpringApplication.run(AppInitializer.class, args);
		
		
	}
	
	 @PostConstruct
	    public void registerInstance() {
			dataAggregationService.pollMessageFromQueue();	
			}

}
