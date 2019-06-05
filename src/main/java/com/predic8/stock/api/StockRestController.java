package com.predic8.stock.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.predic8.stock.model.Stock;
import com.predic8.stock.event.Operation;
import java.util.Collection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RequestMapping("/stocks")
@RestController
public class StockRestController {
	private final Map<String, Stock> stocks;

	private ObjectMapper mapper;

	private KafkaTemplate<String, Operation> kafka;

	public StockRestController(Map<String, Stock> articles, ObjectMapper mapper, KafkaTemplate<String, Operation> kafka) {
		this.stocks = articles;
		this.mapper = mapper;
		this.kafka = kafka;
	}

	@GetMapping
	public Collection<Stock> index() {
		return stocks.values();
	}

	@GetMapping("/count")
	public long count() {
		return stocks.size();
	}

	@GetMapping("/{id}")
	public Stock getStock(@PathVariable String id) {
		return stocks.get(id);
	}

	@PutMapping
	public void setStock(@RequestBody Stock stock) {
		Stock availableStock = stocks.get(stock.getUuid());
		if(availableStock != null) {
			//stock.setUuid(availableStock.getUuid());
			Operation op = new Operation("article", "upsert", mapper.valueToTree(stock));
			kafka.send(new ProducerRecord<>("shop", op));
		}
	}
}