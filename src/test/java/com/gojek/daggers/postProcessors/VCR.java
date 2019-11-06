package com.gojek.daggers.postProcessors;

import com.github.tomakehurst.wiremock.WireMockServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class VCR {
    public void main() throws IOException {
        WireMockServer wireMockServer = new WireMockServer(8081);
        wireMockServer.start();
        wireMockServer.startRecording("http://10.120.2.212:9200");
        URL url = new URL("http://localhost:8081/customers/customer/2129");
        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("GET");
        httpURLConnection.getResponseCode();
        wireMockServer.stopRecording();
        wireMockServer.stop();
    }
}
