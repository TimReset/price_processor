package com.price.processor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PriceProcessorConcurrencyImpl implements PriceProcessor, AutoCloseable {

    private final static String REMOVED = "";

    private static class SubscribeLocalData {

        private final BlockingDeque<String> currencyPairQueue = new LinkedBlockingDeque<>();
        private final Set<String> currencyPairInQueue = new CopyOnWriteArraySet<>();
        private final ConcurrentMap<String, Double> sentCurrencyPriceMap = new ConcurrentHashMap<>();
    }

    private final ConcurrentMap<PriceProcessor, SubscribeLocalData> subscribes = new ConcurrentHashMap<>();

    private final BlockingDeque<PriceProcessor> subscribesQueue = new LinkedBlockingDeque<>();

    private final ConcurrentMap<String, Double> currencyPairAndPriceMap = new ConcurrentHashMap<>();

    private final ExecutorService executorService;

    public PriceProcessorConcurrencyImpl(int workerCount) {
        final ThreadFactory threadFactory = new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("PriceProcessor-" + threadNumber.incrementAndGet());
                return thread;
            }
        };

        executorService = Executors.newFixedThreadPool(workerCount, threadFactory);
        for (int i = 0; i < workerCount; i++) {
            executorService.submit(new Worker());
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        if (REMOVED.equals(ccyPair)) {
            throw new IllegalArgumentException("Incorrect ccyPair: " + ccyPair);
        } else {
            currencyPairAndPriceMap.put(ccyPair, rate);
            for (Map.Entry<PriceProcessor, SubscribeLocalData> priceProcessorSubscribeLocalDataEntry : subscribes.entrySet()) {
                if (!priceProcessorSubscribeLocalDataEntry.getValue().currencyPairInQueue.contains(ccyPair)) {
                    priceProcessorSubscribeLocalDataEntry.getValue().currencyPairQueue.addLast(ccyPair);
                }
            }
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        subscribes.put(priceProcessor, new SubscribeLocalData());
        subscribesQueue.addLast(priceProcessor);
    }

    @Override
    public void unsubscribe(PriceProcessor priceProcessor) {
        SubscribeLocalData subscribeLocalData = subscribes.remove(priceProcessor);
        if (subscribeLocalData != null) {
            subscribeLocalData.currencyPairQueue.addFirst(REMOVED);
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    final PriceProcessor priceProcessor = subscribesQueue.takeFirst();
                    final SubscribeLocalData subscribeLocalData = subscribes.get(priceProcessor);
                    if (subscribeLocalData != null) {
                        while (true) {
                            final String ccyPair = subscribeLocalData.currencyPairQueue.takeFirst();
                            subscribeLocalData.currencyPairInQueue.remove(ccyPair);
                            if (REMOVED.equals(ccyPair)) {
                                break;
                            } else {
                                final Double oldValue = subscribeLocalData.sentCurrencyPriceMap.get(ccyPair);
                                final Double currentValue = currencyPairAndPriceMap.get(ccyPair);
                                if (oldValue == null || Double.compare(oldValue, currentValue) != 0) {
                                    priceProcessor.onPrice(ccyPair, currentValue);
                                    subscribeLocalData.sentCurrencyPriceMap.put(ccyPair, currentValue);
                                    subscribesQueue.addLast(priceProcessor);
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
