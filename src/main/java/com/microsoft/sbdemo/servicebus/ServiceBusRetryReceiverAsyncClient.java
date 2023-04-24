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
import reactor.util.retry.Retry;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ServiceBusRetryReceiverAsyncClient implements AutoCloseable {
    private static final int SCHEDULER_INTERVAL_IN_SECONDS = 30;
    private static final Duration RETRY_WAIT_TIME = Duration.ofSeconds(4);
    private final AtomicReference<ServiceBusSessionReceiverAsyncClient> asyncClient = new AtomicReference<>();
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private Disposable monitorDisposable;

    private boolean wasStopped = false;
    private final ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder serviceBusClientBuilder;

    private final String sessionId;

    public ServiceBusRetryReceiverAsyncClient(
            ServiceBusClientBuilder.ServiceBusSessionReceiverClientBuilder serviceBusClientBuilder,
            String sessionId) {
        this.serviceBusClientBuilder = serviceBusClientBuilder;
        ServiceBusSessionReceiverAsyncClient client = serviceBusClientBuilder.buildAsyncClient();
        this.asyncClient.set(client);
        this.sessionId = sessionId;
    }

    public synchronized void start() {
        if (isRunning.getAndSet(true)) {
            log.info("ServiceBusRetryReceiverAsyncClient is already running");
            return;
        }

        if (wasStopped) {
            wasStopped = false;
        }

        if (asyncClient.get() == null) {
            ServiceBusSessionReceiverAsyncClient newReceiverClient = serviceBusClientBuilder.buildAsyncClient();

            asyncClient.set(newReceiverClient);
        }

        receiveMessages();

        if (monitorDisposable == null) {
            monitorDisposable = Schedulers.boundedElastic().schedulePeriodically(() -> {
                                                                            boolean isChannelClosed = isChannelClosed(Objects.requireNonNull(
                                                                                             this.asyncClient.get().acceptSession(sessionId).block()));

                                                                                     if (isChannelClosed) {
                                                                                         log.error("Channel is closed");
                                                                                         restartMessageReceiver();
                                                                                     }
                                                                                 }, SCHEDULER_INTERVAL_IN_SECONDS, SCHEDULER_INTERVAL_IN_SECONDS,
                                                                                 TimeUnit.SECONDS);
        }
    }

    private synchronized void receiveMessages() {
        if (!isRunning()) {
            return;
        }
        Flux<ServiceBusReceivedMessage> sessionMessages = Flux.usingWhen(
                this.asyncClient.get().acceptSession(sessionId),
                receiver -> receiver.receiveMessages(),
                receiver -> Mono.fromRunnable(() -> receiver.close())).retryWhen(
                Retry.fixedDelay(Long.MAX_VALUE, RETRY_WAIT_TIME)
                     .filter(throwable -> {
                         if (!isRunning.get()) {
                             return false;
                         }
                         log.warn("Current LowLevelClient's retry exhausted or a non-retryable error occurred.",
                                  throwable);
                         return true;
                     }));

        Disposable messageSubscription = sessionMessages.doOnError(error -> {
            log.error("Error occurred while receiving messages", error);
        }).subscribe(message -> {
            String body = message.getBody().toString();
            System.out.printf("Received Sequence #: %s. Contents: %s%n",
                              message.getSequenceNumber(), message.getBody());
            try {
                Thread.sleep(600000l);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            complete(message);
        }, error -> log.error(error.toString()));

    }

    public synchronized boolean isRunning() {
        return isRunning.get();
    }

    private synchronized void restartMessageReceiver() {
        log.error("Restarting message receiver");
        ServiceBusSessionReceiverAsyncClient receiverClient = asyncClient.get();
        receiverClient.close();
        ServiceBusSessionReceiverAsyncClient newReceiverClient = serviceBusClientBuilder.buildAsyncClient();

        asyncClient.set(newReceiverClient);
        receiveMessages();
    }

    @Override
    public synchronized void close() {
        log.error("close()");
        isRunning.set(false);

        if (monitorDisposable != null) {
            monitorDisposable.dispose();
            monitorDisposable = null;
        }
        if (asyncClient.get() != null) {
            asyncClient.get().close();
            asyncClient.set(null);
        }
    }

    private static boolean isChannelClosed(ServiceBusReceiverAsyncClient client) {
        boolean isChannelClosed = false;
        try {
            Method amqpChannel = null;
            amqpChannel = client.getClass().getDeclaredMethod("isConnectionClosed");
            amqpChannel.setAccessible(true);

            isChannelClosed = (boolean) amqpChannel.invoke(client);
        } catch (NoSuchMethodException | InvocationTargetException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return isChannelClosed;
    }

    public void complete(ServiceBusReceivedMessage message) {
        final ServiceBusReceiverAsyncClient lowLevelClient =
                this.asyncClient.get().acceptSession(sessionId).block();

        lowLevelClient.complete(message).block();

    }

    public void abandon(ServiceBusReceivedMessage message) {
        final ServiceBusReceiverAsyncClient lowLevelClient =
                this.asyncClient.get().acceptSession(sessionId).block();

        lowLevelClient.abandon(message).block();
    }

    public void deadLetter(ServiceBusReceivedMessage message) {
        final ServiceBusReceiverAsyncClient lowLevelClient =
                this.asyncClient.get().acceptSession(sessionId).block();

        lowLevelClient.deadLetter(message).block();
    }
}
