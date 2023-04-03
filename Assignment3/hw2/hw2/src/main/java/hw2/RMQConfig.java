package hw2;

class RMQConfig {
  private final String host;
  private final int port;
  private final String virtualHost;
  private final String username;
  private final String password;

  public RMQConfig(String host, int port, String virtualHost, String username, String password) {
    this.host = host;
    this.port = port;
    this.virtualHost = virtualHost;
    this.username = username;
    this.password = password;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getVirtualHost() {
    return virtualHost;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}