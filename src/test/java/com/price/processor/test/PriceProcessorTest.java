package com.price.processor.test;

import com.price.processor.PriceProcessor;
import com.price.processor.PriceProcessorConcurrencyImpl;
import com.price.processor.PriceProcessorSimpleImplementation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class PriceProcessorTest {

    static class PriceProcessorMock implements PriceProcessor {

        private final ReentrantLock lock = new ReentrantLock();

        public final List<Map.Entry<String, Double>> prices = new CopyOnWriteArrayList<>();

        @Override
        public void onPrice(String ccyPair, double rate) {
            lock.lock();
            try {
                prices.add(Map.entry(ccyPair, rate));
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void subscribe(PriceProcessor priceProcessor) {
            throw new RuntimeException("unsupported subscribe");
        }

        @Override
        public void unsubscribe(PriceProcessor priceProcessor) {
            throw new RuntimeException("unsupported unsubscribe");
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }
    }

    @Test
    public void testSimple() {
        final PriceProcessor priceProcessor = new PriceProcessorSimpleImplementation();
        final PriceProcessorMock priceProcessorMock1 = new PriceProcessorMock();
        final PriceProcessorMock priceProcessorMock2 = new PriceProcessorMock();
        priceProcessor.subscribe(priceProcessorMock1);
        priceProcessor.onPrice("test", 1);
        priceProcessor.subscribe(priceProcessorMock2);
        priceProcessor.onPrice("test1", 2);
        priceProcessor.onPrice("test2", 3);

        {
            assertEquals(3, priceProcessorMock1.prices.size());
            assertEquals(Map.entry("test", 1d), priceProcessorMock1.prices.get(0));
            assertEquals(Map.entry("test1", 2d), priceProcessorMock1.prices.get(1));
            assertEquals(Map.entry("test2", 3d), priceProcessorMock1.prices.get(2));
        }
        {
            assertEquals(2, priceProcessorMock2.prices.size());
            assertEquals(Map.entry("test1", 2d), priceProcessorMock2.prices.get(0));
            assertEquals(Map.entry("test2", 3d), priceProcessorMock2.prices.get(1));
        }
    }

    @Test
    public void testConcurrency() {
        final PriceProcessorConcurrencyImpl priceProcessor = new PriceProcessorConcurrencyImpl(2);
        final PriceProcessorMock priceProcessorMock1 = new PriceProcessorMock();
        final PriceProcessorMock priceProcessorMock2 = new PriceProcessorMock();
        priceProcessor.subscribe(priceProcessorMock1);
        priceProcessor.onPrice("test", 1);
        priceProcessor.subscribe(priceProcessorMock2);
        priceProcessor.onPrice("test1", 2);
        priceProcessor.onPrice("test2", 3);
        waitUnit(() -> priceProcessorMock1.prices.size() == 3);
        waitUnit(() -> priceProcessorMock2.prices.size() == 2);
        {
            assertEquals(3, priceProcessorMock1.prices.size());
            assertEquals(Map.entry("test", 1d), priceProcessorMock1.prices.get(0));
            assertEquals(Map.entry("test1", 2d), priceProcessorMock1.prices.get(1));
            assertEquals(Map.entry("test2", 3d), priceProcessorMock1.prices.get(2));
        }
        {
            assertEquals(2, priceProcessorMock2.prices.size());
            assertEquals(Map.entry("test1", 2d), priceProcessorMock2.prices.get(0));
            assertEquals(Map.entry("test2", 3d), priceProcessorMock2.prices.get(1));
        }
        priceProcessor.close();
    }

    @Test
    public void testSlowSubscribe() {
        final PriceProcessorConcurrencyImpl priceProcessor = new PriceProcessorConcurrencyImpl(2);
        final PriceProcessorMock priceProcessorMock1 = new PriceProcessorMock();
        final PriceProcessorMock priceProcessorMock2 = new PriceProcessorMock();
        priceProcessor.subscribe(priceProcessorMock1);
        priceProcessor.subscribe(priceProcessorMock2);
        priceProcessor.onPrice("test", 1);

        waitUnit(() -> priceProcessorMock1.prices.size() == 1);
        waitUnit(() -> priceProcessorMock2.prices.size() == 1);

        {
            assertEquals(Map.entry("test", 1d), priceProcessorMock1.prices.get(0));
            assertEquals(Map.entry("test", 1d), priceProcessorMock2.prices.get(0));
        }

        priceProcessorMock1.lock();

        {
            priceProcessor.onPrice("test", 2d);
            waitUnit(() -> priceProcessorMock2.prices.size() == 2);
            assertEquals(1, priceProcessorMock1.prices.size());
            assertEquals(2, priceProcessorMock2.prices.size());
            assertEquals(Map.entry("test", 2d), priceProcessorMock2.prices.get(1));
        }
        {
            priceProcessor.onPrice("test1", 3d);
            waitUnit(() -> priceProcessorMock2.prices.size() == 3);
            assertEquals(1, priceProcessorMock1.prices.size());
            assertEquals(3, priceProcessorMock2.prices.size());
            assertEquals(Map.entry("test1", 3d), priceProcessorMock2.prices.get(2));
        }

        priceProcessorMock1.unlock();
        {
            waitUnit(() -> priceProcessorMock1.prices.size() == 3);
            assertEquals(3, priceProcessorMock1.prices.size());
            assertEquals(3, priceProcessorMock2.prices.size());
            assertEquals(Map.entry("test", 2d), priceProcessorMock1.prices.get(1));
            assertEquals(Map.entry("test1", 3d), priceProcessorMock1.prices.get(2));
        }
    }


    @Test
    public void testLastPrice() {
        final PriceProcessorConcurrencyImpl priceProcessor = new PriceProcessorConcurrencyImpl(2);
        final PriceProcessorMock priceProcessorMock1 = new PriceProcessorMock();
        priceProcessor.subscribe(priceProcessorMock1);
        double lastPrice = 4;
        priceProcessorMock1.lock();
        try {
            priceProcessor.onPrice("test", 1);
            Thread.yield();
            priceProcessor.onPrice("test", 2);
            Thread.yield();
            priceProcessor.onPrice("test", 3);
            Thread.yield();
            priceProcessor.onPrice("test", lastPrice);
            Thread.yield();
        } finally {
            priceProcessorMock1.unlock();
        }
        waitUnit(() -> !priceProcessorMock1.prices.isEmpty());
        //check last price
        assertEquals(Map.entry("test", lastPrice), priceProcessorMock1.prices.get(priceProcessorMock1.prices.size() - 1));
    }

    static void waitUnit(BooleanSupplier condition) {
        final long timoutMs = 500;
        final long start = System.currentTimeMillis();
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() - start > timoutMs) {
                throw new IllegalStateException("Condition is false after " + timoutMs + " ms");
            }
        }
    }

}
