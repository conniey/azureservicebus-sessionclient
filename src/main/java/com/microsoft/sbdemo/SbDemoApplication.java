package com.microsoft.sbdemo;

import com.microsoft.sbdemo.servicebus.ServiceBusRetryReceiverAsyncClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SbDemoApplication implements CommandLineRunner {

    @Autowired
    ServiceBusRetryReceiverAsyncClient serviceBusRetryReceiverAsyncClient;

    public static void main(String[] args) {
        SpringApplication.run(SbDemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        serviceBusRetryReceiverAsyncClient.start();
    }
}
