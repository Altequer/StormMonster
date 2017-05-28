package com.spnotes.storm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Spitter implements IRichBolt{
	// Criar instância para OutputCollector que coleta e emite tuplas para produzir saída.
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}
	
	//Faz o processamento da tupla recibida como parâmetro.
	@Override
	public void execute(Tuple tupla) {
		String valorTupla = tupla.getString(0);
		String[] Todaspalavras = valorTupla.split(" ");
		for(String palavra: Todaspalavras){
			palavra = palavra.trim();
			if(!palavra.isEmpty()){
				palavra = palavra.toLowerCase();
				collector.emit(new Values(palavra));
			}
		}
		//Reconhece que uma tupla foi processada.
		collector.ack(tupla);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	//Retorna tupla para processamento
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
