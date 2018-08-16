package liu.xiao.zor.embeddedkafka;

import java.util.Properties;

public class EmbeddedKafkaBuilder {

    private int numServer;
    private Properties serverProperties;

    public EmbeddedKafka build() throws Exception {
        return new EmbeddedKafkaFlinkKafkaTestEnvImpl(this);
    }

    public EmbeddedKafkaBuilder numServer(int numServer) {
        this.numServer = numServer;
        return this;
    }

    public EmbeddedKafkaBuilder serverProperties(Properties serverProperties) {
        this.serverProperties = serverProperties;
        return this;
    }

    public int getNumServer() {
        return numServer;
    }

    public void setNumServer(int numServer) {
        this.numServer = numServer;
    }

    public Properties getServerProperties() {
        return serverProperties;
    }

    public void setServerProperties(Properties serverProperties) {
        this.serverProperties = serverProperties;
    }
}
