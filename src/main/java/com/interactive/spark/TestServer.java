package com.interactive.spark;



import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class TestServer {
    private static final Logger logger = Logger.getLogger(TestServer.class.getName());
    private static Server jettyServer;

    public static void startServer() {
        QueuedThreadPool threadPool =
                new QueuedThreadPool(7);
        jettyServer = new Server(threadPool);
        ServerConnector connector = new ServerConnector(jettyServer);
        connector.setIdleTimeout(120000);
        connector.setPort(8359);
        jettyServer.setConnectors(new Connector[]{connector});
        jettyServer.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize",
                2000000);
        jettyServer.setHandler(Handler.getInstance());
        try {
            jettyServer.start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            logger.error(e);
        }

    }

    public static void waitForServerShutdown() {
        try {
            jettyServer.join();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void stopServer() {
        try {
            if (jettyServer != null)
                jettyServer.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TestServer.startServer();
        TestServer.waitForServerShutdown();
    }
}

