
package com.excellenceengineeringsolutions.profiler.output;

import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.grpc.GrpcTransportOptions;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * CREATE TABLE LOG (
 * timestamp INT64,
 * duration INT64,
 * `end` INT64,
 * message STRING(MAX),
 * start INT64,
 * ) PRIMARY KEY (timestamp)
 */
public class SpannerDump
{
  public static final String PROJECT_ID = "cloud-spanner";
  public static final String LOG_FOLDER = "/advdata/trace";
  public static final String LOG_FILE = LOG_FOLDER + "/error-interceptor.log";
  protected final static String INSTANCE_ID = "eu-instance";
  protected final static String DATABASE_ID = "vasile";
  protected static long sessionId;
  private static DatabaseClient client;
  private static ExecutorService service = Executors.newSingleThreadExecutor(new ThreadFactory()
  {
    public Thread newThread(Runnable r)
    {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
    }
  });

  private static PrintWriter logger;
  private static Spanner spanner;

  private static PrintWriter getLogger()
  {
    if ( logger == null )
    {
      if ( new File(LOG_FOLDER).exists() )
      {
        try
        {
          logger = new PrintWriter(new FileOutputStream(
            new File(LOG_FILE),
            true));

          Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
          {
            @Override
            public void run()
            {
              if ( getLogger() != null )
              {
                getLogger().flush();
                getLogger().close();
              }
            }
          }
          ));
        }
        catch ( FileNotFoundException e )
        {
          e.printStackTrace();
        }
      }
    }
    return logger;
  }

  public static String getCredentialsFile()
  {    
    String env = System.getenv("SPANNER_SECURITY_FILE");
    return env != null ? env : "/etc/google/security/credentials.json";
  }

  public static void trace(final String method,final String arguments,
                           final String stacktrace, final long start, final long end, final long duration)
  {
    service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          System.out.println("write");
          long currentTimeMillis = System.currentTimeMillis();
          log(String.format("timestamp: %d, method: %s,arguments: %s,stacktrace: %s, start: %d, end: %d, duration: %d\n"
            , currentTimeMillis, method,arguments,stacktrace,
            start, end, duration
          ));

          getClient().write(
            Arrays.asList(Mutation.newInsertBuilder("LOG_PAW")
              .set("timestamp").to(Timestamp.of(new java.util.Date(currentTimeMillis)))
              .set("sessionId").to(sessionId)
              .set("method").to(method)
              .set("arguments").to(arguments)
              .set("stacktrace").to(stacktrace)
              .set("start").to(Timestamp.of(new java.util.Date(start)))
              .set("end").to(Timestamp.of(new java.util.Date(end)))
              .set("duration").to(duration)
              .build()
            ));
          System.out.println("done write");
        }
        catch ( Throwable ex )
        {
          log(ex);
        }
      }
    });
  }

  private static ServiceAccountJwtAccessCredentials buildCredentials()
  {
    try
    {
      String credentialsFile = getCredentialsFile();
      System.out.println(credentialsFile);
      log(Files.readLines(new File(credentialsFile), Charsets.UTF_8).toString());
      ServiceAccountJwtAccessCredentials serviceAccountJwtAccessCredentials = ServiceAccountJwtAccessCredentials.fromStream(new FileInputStream(getCredentialsFile()));
      log(serviceAccountJwtAccessCredentials.toString());
      return serviceAccountJwtAccessCredentials;
    }
    catch ( Exception e )
    {
      log(e);
    }
    return null;
  }

  public static void log(Throwable e){
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    String sStackTrace = sw.toString(); // stack trace as a string
    log(sStackTrace);
  }

  public static void log(String e)
  {
    String message = System.currentTimeMillis() + ":" + e.toString() + "\n";
    if (getLogger()!=null){
      getLogger().write(message);
      getLogger().flush();
    }else{
      System.out.println(message);
    }
  }

  public static void main(String[] args)
  {
    ResultSet resultSet = getClient().singleUse().executeQuery(Statement.of("select 1 from information_schema.tables limit 1"));
    resultSet.next();
    System.out.println("count " + resultSet.getColumnCount());

  }

  public static DatabaseClient getClient()
  {
    if ( client == null )
    {
      sessionId = System.currentTimeMillis();
      SpannerOptions.Builder spannerOptionsBuilder = SpannerOptions.newBuilder()
        .setProjectId(PROJECT_ID)
        .setCredentials(buildCredentials())
        .setSessionPoolOption(
          SessionPoolOptions.newBuilder()
            .setFailIfPoolExhausted()
            .setMaxSessions(10)
            .setMinSessions(1)
            .setWriteSessionsFraction(0.9f)
            .build())
        .setNumChannels(1)
        .setTransportOptions(GrpcTransportOptions.newBuilder().build());

      SpannerOptions options = spannerOptionsBuilder.build();
      spanner = options.getService();
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          spanner.close();
        }
      }
      ));
      client = spanner.getDatabaseClient(DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID));
    }
    return client;
  }
}
