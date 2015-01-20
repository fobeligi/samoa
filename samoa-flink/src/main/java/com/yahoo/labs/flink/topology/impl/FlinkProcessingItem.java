package com.yahoo.labs.flink.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.Lists;
import com.yahoo.labs.flink.Utils;
import com.yahoo.labs.flink.Utils.Partitioning;
import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

import java.io.Serializable;
import java.util.List;


public class FlinkProcessingItem extends StreamInvokable<SamoaType, SamoaType> implements ProcessingItem, FlinkComponent, Serializable {

	private final Processor processor;
	private final transient StreamExecutionEnvironment env;
	private final SamoaDelegateFunction fun;
	private transient DataStream<SamoaType> inStream;
	private transient DataStream<SamoaType> outStream;
	private transient List<FlinkStream> outputStreams = Lists.newArrayList();
	private transient List<Tuple2<FlinkStream, Partitioning>> inputStreams = Lists.newArrayList();
	private int parallelism;


	public FlinkProcessingItem(StreamExecutionEnvironment env, Processor proc) {
		this(env, proc, 1);
	}

	public FlinkProcessingItem(StreamExecutionEnvironment env, Processor proc, int parallelism) {
		this(env, new SamoaDelegateFunction(proc), proc, parallelism);
	}

	public FlinkProcessingItem(StreamExecutionEnvironment env, SamoaDelegateFunction fun, Processor proc, int parallelism) {
		super(fun);
		this.env = env;
		this.fun = fun;
		this.processor = proc;
		this.parallelism = parallelism;
	}

	public Stream createStream() {
		FlinkStream generatedStream = new FlinkStream(this);
		outputStreams.add(generatedStream);
		return generatedStream;
	}

	public void putToStream(ContentEvent data, Stream targetStream) {
		collector.collect(SamoaType.of(data, targetStream.getStreamId()));
	}


	@Override
	public void initialise() {
		for (Tuple2<FlinkStream, Partitioning> inputStream : inputStreams) {
			if (inStream == null) {
				inStream = Utils.subscribe(inputStream.f0.getOutStream(), inputStream.f1);
			} else {
				inStream = inStream.merge(Utils.subscribe(inputStream.f0.getOutStream(), inputStream.f1));
			}
		}
		outStream = inStream.transform("samoaProcessor", inStream.getType(), this).setParallelism(parallelism);
	}

	@Override
	public boolean canBeInitialised() {
		for (Tuple2<FlinkStream, Partitioning> inputStream : inputStreams) {
			if (!inputStream.f0.isInitialised()) return false;
		}
		return true;
	}

	@Override
	public boolean isInitialised() {
		return outStream != null;
	}

	@Override
	public Processor getProcessor() {
		return processor;
	}

	@Override
	public void invoke() throws Exception {
		while (readNext() != null) {
			fun.processEvent(nextRecord.getObject().f1);
		}

	}

	@Override
	public ProcessingItem connectInputShuffleStream(Stream inputStream) {
		inputStreams.add(new Tuple2<>((FlinkStream) inputStream, Partitioning.SHUFFLE));
		return this;
	}

	@Override
	public ProcessingItem connectInputKeyStream(Stream inputStream) {
		inputStreams.add(new Tuple2<>((FlinkStream) inputStream, Partitioning.GROUP));
		return this;
	}

	@Override
	public ProcessingItem connectInputAllStream(Stream inputStream) {
		inputStreams.add(new Tuple2<>((FlinkStream) inputStream, Partitioning.ALL));
		return this;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public List<FlinkStream> getOutputStreams() {
		return outputStreams;
	}

	public DataStream<SamoaType> getOutStream() {
		return this.outStream;
	}

	public void setOutStream(SplitDataStream outStream) {
		this.outStream = outStream;
	}

	static class SamoaDelegateFunction implements Function, Serializable {
		private final Processor proc;

		SamoaDelegateFunction(Processor proc) {
			this.proc = proc;
		}

		public void processEvent(ContentEvent event) {
			proc.process(event);
		}
	}

}
