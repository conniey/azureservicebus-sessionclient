package com.microsoft.sbdemo.servicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverAsyncClient;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ServiceBusRetryReceiverAsyncClient implements AutoCloseable {
    private static final int SCHEDULER_INTERVAL_IN_SECONDS = 30;
    private static final Duration TIMEOUT = Duration.ofSeconds(SCHEDULER_INTERVAL_IN_SECONDS / 2);
    private static final Duration RETRY_WAIT_TIME = Duration.ofSeconds(4);
    private final AtomicBoolean isRunning = new AtomicBoolean();

    private final AtomicBoolean isCurrentReceiverDisposed = new AtomicBoolean(false);
    private Disposable monitorDisposable;

    private final ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder serviceBusClientBuilder;

    private final String sessionId;

    public ServiceBusRetryReceiverAsyncClient(
            ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder serviceBusClientBuilder,
            String sessionId) {
        this.serviceBusClientBuilder = serviceBusClientBuilder;
        this.sessionId = sessionId;
    }

    public void start() {
        if (isRunning.getAndSet(true)) {
            log.info("ServiceBusRetryReceiverAsyncClient is already running");
            return;
        }

        getClient().subscribe(client -> {
            log.info("Acquired session client. Starting receive messages.");
            receiveMessages();
        });


        if (monitorDisposable == null) {
            monitorDisposable = Schedulers.boundedElastic().schedulePeriodically(() -> {
                if (isCurrentReceiverDisposed.get()) {
                    log.info("Restarting message receiver.");
                    receiveMessages();
                }
            }, SCHEDULER_INTERVAL_IN_SECONDS, SCHEDULER_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
        }
    }

    private void receiveMessages() {
        if (!isRunning()) {
            return;
        }

        Flux<ServiceBusReceivedMessage> sessionMessages = Flux.usingWhen(
                getClient().doOnNext(client -> {
                    isCurrentReceiverDisposed.set(false);
                }),
                receiver -> {
                    return receiver.receiveMessages();
                },
                receiver -> {
                    return Mono.fromRunnable(() -> {
                        log.info("Disposing of current receiver.");
                        isCurrentReceiverDisposed.set(false);

                        receiver.close();
                    });
                });

        Disposable messageSubscription = sessionMessages.subscribe(message -> {
            String body = message.getBody().toString();
            System.out.printf("Received Sequence #: %s. Contents: %s%n", message.getSequenceNumber(), body);
            try {
                Thread.sleep(600000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            complete(message);
        }, error -> {
            // Terminal signal
            log.error("Error while receiving messages.", error);
            isCurrentReceiverDisposed.set(false);
        }, () -> {
            // Terminal signal
            log.info("Receive operation completed.");
            isCurrentReceiverDisposed.set(false);
        });
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    private void restartMessageReceiver() {
        receiveMessages();
    }

    @Override
    public void close() {
        if (!isRunning.getAndSet(false)) {
            log.debug("Already closed.");
            return;
        }

        log.error("close()");

        if (!isCurrentReceiverDisposed.get()) {
            ServiceBusReceiverAsyncClient existingClient = getClient().block(TIMEOUT);

            if (existingClient != null) {
                existingClient.close();
            }
        }
    }

    public void complete(ServiceBusReceivedMessage message) {
        getClient().map(client -> client.complete(message)).block(TIMEOUT);

    }

    public void abandon(ServiceBusReceivedMessage message) {
        getClient().map(client -> client.abandon(message)).block(TIMEOUT);
    }

    public void deadLetter(ServiceBusReceivedMessage message) {
        getClient().map(client -> client.deadLetter(message)).block(TIMEOUT);
    }

    /**
     * Either gets a new client or the cached one.
     */
    public Mono<ServiceBusReceiverAsyncClient> getClient() {
        return Mono.defer(() -> {
            log.info("Creating new service bus session client.");
            ServiceBusSessionReceiverAsyncClient client = serviceBusClientBuilder.buildAsyncClient();

            return client.acceptSession(sessionId);
        }).cacheInvalidateIf(client -> {
            return isCurrentReceiverDisposed.get();
        });
    }
}
