/**
 * 
 */
package org.misi.tbda.dataStreams.notifiers;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ProxyServer;
import com.ning.http.client.websocket.WebSocket;
import com.ning.http.client.websocket.WebSocketTextListener;
import com.ning.http.client.websocket.WebSocketUpgradeHandler;

/**
 * @author Matias
 * 
 */
public class NotifierWebSocket {
	AsyncHttpClient c;
	WebSocket websocket;

	public NotifierWebSocket() {
		AsyncHttpClientConfig cf = new AsyncHttpClientConfig.Builder().build();
		c = new AsyncHttpClient(cf);

	}

	public void connect(String url) {

		try {
			websocket = c
					.prepareGet(url)
					.execute(
							new WebSocketUpgradeHandler.Builder()
									.addWebSocketListener(
											new WebSocketTextListener() {

												@Override
												public void onMessage(
														String message) {
												}

												@Override
												public void onOpen(
														WebSocket websocket) {
													websocket
															.sendTextMessage("...");
												}

												@Override
												public void onClose(
														WebSocket websocket) {

												}

												@Override
												public void onError(Throwable t) {
												}

												@Override
												public void onFragment(
														String arg0,
														boolean arg1) {
													// TODO Auto-generated
													// method stub

												}
											}).build()).get();
		} catch (InterruptedException e) {

			e.printStackTrace();
		} catch (ExecutionException e) {

			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public void send(String message) {
		websocket.sendTextMessage(message);
	}

}
