package com.microsoft.sbdemo.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.microsoft.sbdemo.servicebus.ServiceBusRetryReceiverAsyncClient;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class AzureServiceBusConfiguration {

    private final ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder serviceBusClientBuilder;

    @Value("${sessionId:Preenu}")
    private String sessionId;

    @Bean
    public ServiceBusRetryReceiverAsyncClient serviceBusRetryReceiverAsyncClient() {
        serviceBusClientBuilder.disableAutoComplete();
        return new ServiceBusRetryReceiverAsyncClient(serviceBusClientBuilder, sessionId);
    }
}
