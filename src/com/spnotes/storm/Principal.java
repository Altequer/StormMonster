package com.spnotes.storm;
import com.spnotes.storm.bolts.Counter;
import com.spnotes.storm.bolts.Spitter;
import com.spnotes.storm.spouts.Spout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class Principal {
	
	/*
	 * A idéia 
	 * básica por trás do programa é que carregue o arquivo.txt como entrada e 
	 * passa para Spout.java, que lê o arquivo de uma linha em um Tempo e 
	 * passa para Storm para processamento. Storm irá passar cada linha 
	 * para Spitter.java, esta classe é responsável por dividir a linha em 
	 * várias palavras e passá-los de volta para Storm para processamento, a 
	 * última parte é Counter.java que leva cada uma das palavras e 
	 * mantém um HashMap de palavras. No final, o 
	 * Counter.java irá imprimir todas as palavras para o console
	 */

	public static void main(String[] args) throws Exception{
		//Definir opções de configuração antes de enviar a topologia.
		Config config = new Config();
		//Arquivo que será processado.
		config.put("arquivo", "C:\\Users\\Jonathan\\Desktop\\teste.txt");
		//Para registrar cada mensagem emitida.
		config.setDebug(true);
		//Parâmetro para acelerar o número de mensagens indo para parafuso.
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		//Cria a topologia.
		TopologyBuilder topologia = new TopologyBuilder();
		//Defina que o Spout será um  bico nesta topologia.
		topologia.setSpout("Spout", new Spout());
		//Defina que o Spitter como parafuso nesta topologia.
		topologia.setBolt("Spitter", new Spitter()).shuffleGrouping("Spout");
		//Defina que o Counter como parafuso na topologia.
		topologia.setBolt("Counter", new Counter()).shuffleGrouping("Spitter");
		
		//Cria um cluster local para utilizar no locahost;
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Trabalho", config, topologia.createTopology());
		Thread.sleep(10000);
		
		// Fecha cluster local.
		cluster.shutdown();
	}

}
