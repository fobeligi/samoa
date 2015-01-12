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


import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.IProcessingItem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A SAMOA topology on Apache Flink
 */
public class FlinkTopology extends AbstractTopology {

	private StreamExecutionEnvironment environment;

	public FlinkTopology(String name) {
		super(name);
		this.environment = StreamExecutionEnvironment.getExecutionEnvironment();
	}

	@Override
	public void addProcessingItem(IProcessingItem procItem) {
		super.addProcessingItem(procItem);
	}

	@Override
	public void addProcessingItem(IProcessingItem procItem, int parallelismHint) {
		super.addProcessingItem(procItem, parallelismHint);
	}

	public StreamExecutionEnvironment getEnvironment() {
		return environment;
	}
}
