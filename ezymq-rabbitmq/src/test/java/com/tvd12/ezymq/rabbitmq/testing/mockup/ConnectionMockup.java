package com.tvd12.ezymq.rabbitmq.testing.mockup;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;
import com.tvd12.ezyfox.util.EzyLoggable;

public class ConnectionMockup extends EzyLoggable implements Connection {

	@Override
	public void addShutdownListener(ShutdownListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeShutdownListener(ShutdownListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ShutdownSignalException getCloseReason() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void notifyListeners() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InetAddress getAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getChannelMax() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFrameMax() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHeartbeat() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Object> getClientProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientProvidedName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getServerProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel createChannel() throws IOException {
		return new ChannelMockup();
	}

	@Override
	public Channel createChannel(int channelNumber) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(int closeCode, String closeMessage) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(int timeout) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(int closeCode, String closeMessage, int timeout) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort(int closeCode, String closeMessage) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort(int timeout) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void abort(int closeCode, String closeMessage, int timeout) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addBlockedListener(BlockedListener listener) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeBlockedListener(BlockedListener listener) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearBlockedListeners() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setId(String id) {
		// TODO Auto-generated method stub
		
	}

}
