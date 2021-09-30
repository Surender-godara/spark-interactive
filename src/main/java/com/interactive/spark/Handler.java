package com.interactive.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;


public class Handler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(Handler.class.getName());

    private static Handler handler = new Handler();

    public SparkContextHolder sparkContextHolder;

    private Handler() {
        sparkContextHolder = SparkContextHolder.getInstance();
    }

    public static Handler getInstance() {
        return handler;
    }


    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        long time = System.currentTimeMillis();
        String decodedBaseURI =
                baseRequest.getRequestURI() + "?" + decodeQueryParameter(baseRequest.getQueryString());

        logger.info("Passed base URI=" + decodedBaseURI);

        try {

            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            if (target.startsWith("/registerTable")) {
                registerTable(target, request, response);
            } else if (target.startsWith("/getTableData")) {
                getTableData(target, request, response);
            } else if (target.startsWith("/list")) {
                list(target, request, response);
            } else if (target.startsWith("/execute")) {
                execute(target, request, response);
            } else {
                throw new RuntimeException("");
            }


        } catch (Throwable e) {
            logger.error("While serving request : " + baseRequest.getRequestURI());
            throw e;
        } finally {
            logger.info("Requested URI : " + baseRequest.getRequestURI() + " , taken total : " + (System.currentTimeMillis() - time) + " milliseconds to execute.");
        }
    }

    private void registerTable(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (request.getParameter("tableName") == null) {
            response.getWriter().println(
                    "Require parameter : tableName");
            return;
        }
        String tableName = request.getParameter("tableName").trim();
        SparkSession sparkSession = sparkContextHolder.getSparkSession();
        Dataset<Row> table = sparkSession.read().format("orc").load("test_server/userdata1_orc");
        table.registerTempTable(tableName);
        response.getWriter().println("Table : " + tableName + " Registered Successfully  ");
    }

    private void getTableData(String target, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        if (request.getParameter("tableName") == null) {
            response.getWriter().println(
                    "Require parameter : tableName ");
            return;
        }
        String tableName = request.getParameter("tableName").trim();
        long startime = System.currentTimeMillis();
        String[] tables = sparkContextHolder.getSqlContext().tableNames();
        boolean found = false;
        for (String str : tables) {
            if (str.toLowerCase().equals(tableName.toLowerCase())) {
                found = true;
            }
        }
        if (!found) {
            response.getWriter().println("Table : " + tableName + " does not exists in : " + Arrays.asList(tables));
            return;
        }

        String separator = "|";
        if (request.getParameter("separator") != null) {
            String str = request.getParameter("separator").toString();
            if (str.trim().length() != 0) {
                separator = str.trim();
            }
        }
        Row[] rows = (Row[]) sparkContextHolder.getSqlContext().table(tableName).collect();
        int count = 0;
        for (Row next : rows) {
            for (int i = 0; i < next.size(); i++) {
                response.getWriter().print(next.get(i));
                if (i != (next.size() - 1)) {
                    response.getWriter().print(separator);
                }
            }
            response.getWriter().println();
            count++;
        }

        long time_taken = (System.currentTimeMillis() - startime);
        response.getWriter().println(
                "table = " + tableName + " count = " + count + " time taken = " + time_taken);
    }

    private void list(String target, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String[] tables = sparkContextHolder.getSqlContext().tableNames();
        response.getWriter().println("Available tables are : " + Arrays.asList(tables));
    }

    private void execute(String target, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        if (request.getParameter("query") == null) {
            response.getWriter().println(
                    "Require parameter : query");
            return;
        }
        String query = request.getParameter("query").trim();
        long startime = System.currentTimeMillis();
        String separator = "|";
        if (request.getParameter("separator") != null) {
            String str = request.getParameter("separator").toString();
            if (str.trim().length() != 0) {
                separator = str.trim();
            }
        }


        boolean skipHeader = false;
        if (request.getParameter("skipHeader") != null) {
            String str = request.getParameter("skipHeader").toString();
            if (str.trim().length() != 0) {
                skipHeader = Boolean.parseBoolean(str.trim());
            }
        }

        Dataset<Row> sql = sparkContextHolder.getSqlContext().sql(query);
        final StructType schema = sql.schema();
        String[] strings = schema.fieldNames();

        if (!skipHeader) {
            for (int i = 0; i < strings.length; i++) {
                response.getWriter().print(strings[i]);
                if (i < strings.length - 1) {
                    response.getWriter().print(separator);
                }
            }
            response.getWriter().println();
        }


        Row[] rows = (Row[]) sql.collect();
        int count = 0;
        for (Row next : rows) {
            for (int i = 0; i < next.size(); i++) {
                response.getWriter().print(next.get(i));
                if (i != (next.size() - 1)) {
                    response.getWriter().print(separator);
                }
            }
            response.getWriter().println();
            count++;
        }

        long time_taken = (System.currentTimeMillis() - startime);
        response.getWriter().println(
                "Query = " + query + " count = " + count + " time taken = " + time_taken);
    }


    private String decodeQueryParameter(String string) {
        if (string == null || string.trim().length() == 0) {
            return "";
        }
        try {
            return java.net.URLDecoder.decode(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

    }


}


