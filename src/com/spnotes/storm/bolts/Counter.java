package com.spnotes.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class Counter implements IRichBolt{

	private Integer id;
	private String nome;
	private Map<String, Integer> lista;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.lista = new HashMap<String, Integer>();
		this.collector = collector;
		this.nome = context.getThisComponentId();
		this.id = context.getThisTaskId();
		
	}
	
	//Faz o processamento da tupla recibida como parâmetro.
	@Override
	public void execute(Tuple tupla) {
		String str = tupla.getString(0);
		if(!lista.containsKey(str)){
			lista.put(str, 1);
		}else{
			Integer c = lista.get(str) +1;
			lista.put(str, c);
		}
		collector.ack(tupla);
	}
	
	//Imprimi todas as palavras do arquivo e suas informações
	@Override
	public void cleanup() {
		System.out.println(" Informação ("+ nome + "-"+id +")");
		for(Map.Entry<String, Integer> entry:lista.entrySet()){
			System.out.println(entry.getKey()+" : " + entry.getValue());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
