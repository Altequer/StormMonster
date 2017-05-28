package com.spnotes.storm.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class Spout implements IRichSpout {
	
	//Spout que garante que cada mensagem seja processada pelo menos uma vez.
	private SpoutOutputCollector collector;
	//Arquivo que selecionado
	private FileReader arquivo;
	//Finalizou leitura
	private boolean finalizou = false;
	// Este objeto fornece informações da topologia.
	private TopologyContext contexto;
	
	/*
	 * Carrega arquivo
	 */
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.contexto = context;
			this.arquivo = new FileReader(conf.get("arquivo").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Erro ao ler arquivo "
					+ conf.get("arquivo"));
		}
		this.collector = collector;
	}

	/*
	 *Neste método estamos apenas lendo uma linha do arquivo e passando para tupla.
	 */
	@Override
	public void nextTuple() {
		if (finalizou) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
		String str;
		BufferedReader reader = new BufferedReader(arquivo);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Erro ao tentar passar para tupla", e);
		} finally {
			finalizou = true;
		}

	}
	
	//Retorna tupla para processamento.
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("linha"));
	}
	
	//Fecha arquivo
	@Override
	public void close() {
		try {
			arquivo.close();
		} catch (IOException e) {
			System.out.println("Não foi possível fechar o arquivo");
		}

	}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
