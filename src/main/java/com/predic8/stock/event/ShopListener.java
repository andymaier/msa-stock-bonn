package com.predic8.stock.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.predic8.stock.model.Stock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ShopListener {
	private final ObjectMapper mapper;
	private final NullAwareBeanUtilsBean beanUtils;
	private Map<String, Stock> stocks;

	public ShopListener(ObjectMapper mapper, NullAwareBeanUtilsBean beanUtils, Map<String, Stock> stocks) {
		this.mapper = mapper;
		this.beanUtils = beanUtils;
		this.stocks = stocks;
	}

	@KafkaListener(topics = "shop")
	public void listen(Operation op) throws Exception {
		System.out.println("op = " + op);

		Stock stock = mapper.treeToValue(op.getObject(), Stock.class);

		switch (op.getAction()) {
			case "upsert":
				stocks.put(stock.getUuid(), stock);
				break;
			case "remove":
				stocks.remove(stock.getUuid());
				break;
		}
	}
}