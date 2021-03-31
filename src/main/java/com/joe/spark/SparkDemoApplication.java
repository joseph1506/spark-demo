package com.joe.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkDemoApplication.class, args);
		WordCount.count("E:\\propertysales.csv");
	}

}
