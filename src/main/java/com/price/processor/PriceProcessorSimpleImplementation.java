package com.price.processor;

import java.util.HashSet;
import java.util.Set;

public class PriceProcessorSimpleImplementation implements PriceProcessor {

  private final Set<PriceProcessor> subscribes = new HashSet<>();

  @Override
  public synchronized void onPrice(String ccyPair, double rate) {
    for (PriceProcessor subscribe : subscribes) {
      subscribe.onPrice(ccyPair, rate);
    }
  }

  @Override
  public synchronized void subscribe(PriceProcessor priceProcessor) {
    subscribes.add(priceProcessor);
  }

  @Override
  public synchronized void unsubscribe(PriceProcessor priceProcessor) {
    subscribes.remove(priceProcessor);
  }
}
