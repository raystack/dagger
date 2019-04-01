package com.gojek.daggers.config;


import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;


public class EsAsyncConfigurationProvider {

    public String source;
    public String host;
    public String input_index;
    public String type;
    public String path;
    public String connect_timeout;
    public String retry_timeout;
    public String socket_timeout;
    public String stream_timeout;
    public StencilClient stencilClient;

    public Descriptors.Descriptor getDescriptor(){
        return stencilClient.get(type);
    }
}
