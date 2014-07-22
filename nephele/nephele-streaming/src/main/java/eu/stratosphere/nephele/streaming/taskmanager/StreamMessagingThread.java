/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming.taskmanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.PluginCommunicationProtocol;
import eu.stratosphere.nephele.streaming.StreamingPluginLoader;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class implements a thread to send messages between Qos managers and
 * reporters in an asynchronous fashion. The main reason for asynchronous
 * communication is not to block any threads with RPC call latency.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class StreamMessagingThread extends Thread {

	private static StreamMessagingThread SINGLETON_INSTANCE;

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory
			.getLog(StreamMessagingThread.class);

	/**
	 * The blocking queue which is used to asynchronously exchange data with the
	 * job manager component of this plugin.
	 */
	private final BlockingQueue<AbstractSerializableQosMessage> dataQueue = new LinkedBlockingQueue<AbstractSerializableQosMessage>();

	private final BlockingQueue<InstanceConnectionInfo> connectionInfoQueue = new LinkedBlockingQueue<InstanceConnectionInfo>();

	private HashMap<InstanceConnectionInfo, PluginCommunicationProtocol> proxies = new HashMap<InstanceConnectionInfo, PluginCommunicationProtocol>();

	private StreamMessagingThread() {
		this.setName("StreamMessagingThread");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		try {
			while (!interrupted()) {
				InstanceConnectionInfo connectionInfo = this.connectionInfoQueue
						.take();
				AbstractSerializableQosMessage data = this.dataQueue.take();

				try {
					this.getProxy(connectionInfo).sendData(
							StreamingPluginLoader.STREAMING_PLUGIN_ID, data);
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
					this.proxies.remove(connectionInfo);
				}
			}
		} catch (InterruptedException e) {
		}
	}

	private PluginCommunicationProtocol getProxy(
			InstanceConnectionInfo connectionInfo) throws IOException {
		PluginCommunicationProtocol proxy = this.proxies.get(connectionInfo);
		if (proxy == null) {
			proxy = RPC.getProxy(PluginCommunicationProtocol.class,
					new InetSocketAddress(connectionInfo.getAddress(),
							connectionInfo.getIPCPort()), NetUtils
							.getSocketFactory());
			this.proxies.put(connectionInfo, proxy);
		}

		return proxy;
	}

	/**
	 * Stops the communication thread.
	 */
	void stopMessagingThread() {
		this.interrupt();
	}

	public void sendAsynchronously(
			final InstanceConnectionInfo connectionInfo,
			final AbstractSerializableQosMessage data)
			throws InterruptedException {
		this.dataQueue.put(data);
		this.connectionInfoQueue.put(connectionInfo);
	}

	public synchronized static StreamMessagingThread getInstance() {
		if (SINGLETON_INSTANCE == null) {
			SINGLETON_INSTANCE = new StreamMessagingThread();
			SINGLETON_INSTANCE.start();
		}
		return SINGLETON_INSTANCE;
	}

	public synchronized static void destroyInstance() {
		if (SINGLETON_INSTANCE != null) {
			SINGLETON_INSTANCE.stopMessagingThread();
			SINGLETON_INSTANCE = null;
		}
	}
}
