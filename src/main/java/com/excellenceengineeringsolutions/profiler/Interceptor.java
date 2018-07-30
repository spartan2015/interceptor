
package com.excellenceengineeringsolutions.profiler;

import com.excellenceengineeringsolutions.profiler.output.SpannerDump;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.lang.reflect.Field;

/**
 */
@Aspect
public class Interceptor
{

  public static final int MAX_STACK = 6;

  /*
  @Around("!within(com.excellenceengineeringsolutions.profiler.Interceptor) " +
    "&& (" +
    "(execution(* next(..)) && within(!com.google.cloud.spanner.SpannerImpl.GrpcResultSet)) ||" +
    "(execution(* *(..)) && within(de.siemens.advantage..*))" +
    ")"
  )
  public Object longMethod(ProceedingJoinPoint joinPoint) throws Throwable
  {
    long start = System.currentTimeMillis();
    Object result = null;
    try
    {
      result = joinPoint.proceed();
    }
    finally
    {
      long end = System.currentTimeMillis();
      long duration = end - start;
      if ( duration > 50 )
      {
        String method = joinPoint.toLongString();
        System.out.println(method);
        StringBuilder sb = new StringBuilder();

        addSpannerSql(joinPoint, sb);
        addStack(sb);
        String args = getString(joinPoint.getArgs());
        String stacktrace = sb.toString();

        SpannerDump.trace(method, args, stacktrace, start, end, duration);
      }
    }
    return result;
  }
*/

  @Around("!within(com.excellenceengineeringsolutions.profiler.Interceptor) " +
    "&& (" +
    "(execution(* next(..)) && within(com.google.cloud.spanner.SpannerImpl.GrpcResultSet))" +
    ")"
  )
  public Object sql(ProceedingJoinPoint joinPoint) throws Throwable
  {
    long start = System.currentTimeMillis();
    Object result = null;
    try
    {
      result = joinPoint.proceed();
    }
    finally
    {
      long end = System.currentTimeMillis();
      long duration = end - start;

      String method = joinPoint.toLongString();
      System.out.println(method);
      StringBuilder sb = new StringBuilder();

      addSpannerSql(joinPoint, sb);
      addStack(sb);
      String args = getString(joinPoint.getArgs());
      String stacktrace = sb.toString();

      SpannerDump.trace(method, args, stacktrace, start, end, duration);
    }
    return result;
  }

  @Around("!within(com.excellenceengineeringsolutions.profiler.Interceptor) " +
    "&& (" +
    "(execution(* *(..)) && within(de.siemens.advantage.in.gdm1.gdf.base.impl.GdfBaseHomeCm)) ||" +
    "(execution(* *(..)) && within(de.siemens.advantage.in.gdm1.gdf.base.impl.GdfBaseHomeCp))" +
    ")"
  )
  public Object gdf(ProceedingJoinPoint joinPoint) throws Throwable
  {
    long start = System.currentTimeMillis();
    Object result = null;
    try
    {
      result = joinPoint.proceed();
    }finally{
      long end = System.currentTimeMillis();
      long duration = end - start;
      if (duration > 50)
      {
        String method = joinPoint.toLongString();
        System.out.println(method);
        StringBuilder sb = new StringBuilder();

        addSpannerSql(joinPoint, sb);
        addStack(sb);

        String args = getString(joinPoint.getArgs());
        String stacktrace = sb.toString();

        SpannerDump.trace("[END] " + method, args, stacktrace, start, end, duration);
      }
    }
    return result;
  }

  /*@Around("!within(Interceptor) " +
    "&& (" +
    "(execution(* *(..)) && within(de.siemens.advantage.in.gdm1.gdf.base.impl.GdfBaseHomeCm)) ||" +
    "(execution(* *(..)) && within(de.siemens.advantage.in.gdm1.gdf.base.impl.GdfBaseHomeCp))" +
    ")"
  )
  public Object intercept(ProceedingJoinPoint joinPoint) throws Throwable {
    long start = System.currentTimeMillis();
    String args = getString(joinPoint.getArgs());


    String method = joinPoint.toLongString();
    System.out.println(method);
    StringBuilder sb = new StringBuilder();

    String name = joinPoint.getTarget()!= null ? joinPoint.getTarget().getClass().getSimpleName() : "";
    if ("GrpcResultSet".equals(name))
    {
      sb.append(getFieldValue(joinPoint.getTarget(), "delegate.iterator.stream.val$request"));
    }
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      sb.append(element.toString());
      sb.append("\n");
    }

    String stacktrace = sb.toString();

    try
    {
      SpannerDump.trace("[START]" + method,args,stacktrace, start, start, 0);
    }catch(Exception ex){
      log(ex);
    }
    Object result = null;
    try
    {
      result = joinPoint.proceed();
    }catch(Exception ex){
      log(ex);
      long end = System.currentTimeMillis();
      long duration = end - start;
      SpannerDump.trace(" [EXCEPTION-END] "+ method,args,stacktrace,start,end,duration);
    }
    finally
    {
      long end = System.currentTimeMillis();
      long duration = end - start;
      SpannerDump.trace("[END] "+ method,args,stacktrace,start,end,duration);
    }
    return result;
  }*/

  private String getString(Object[] args)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for ( Object object : args )
    {
      if ( object != null && object.getClass().isArray() )
      {
        sb.append(getString((Object[]) object));
      } else
      {
        sb.append(object);
      }
      sb.append(";");
    }
    sb.append("]");
    return sb.toString();
  }

  public static <T> T getFieldValue(Object target, String fieldName)
  {
    if ( target == null ) return null;
    String[] fields = fieldName.split("\\.");
    for ( String field : fields )
    {
      Field reader = null;
      try
      {
        Class<?> aClass = target.getClass();
        while ( aClass != null )
        {
          try
          {
            reader = aClass.getDeclaredField(field);
          }
          catch ( NoSuchFieldException e )
          {
            //ignore
          }
          if ( reader != null )
          {
            break;
          }
          aClass = aClass.getSuperclass();
        }
        if ( reader != null )
        {
          reader.setAccessible(true);
          target = reader.get(target);
        }
      }
      catch ( IllegalAccessException e )
      {
        e.printStackTrace();
      }
    }
    return (T) target;
  }

  private void addSpannerSql(ProceedingJoinPoint joinPoint, StringBuilder sb) {
    String name = joinPoint.getTarget() != null ? joinPoint.getTarget().getClass().getSimpleName() : "";
    if ( "GrpcResultSet".equals(name) )
    {
      Object fieldValue = getFieldValue(joinPoint.getTarget(), "delegate.iterator.stream.val$request");
      if (fieldValue!=null) {
        sb.append(fieldValue.toString());
      }
    }
  }

  private void addStack(StringBuilder sb)
  {
    for (int i = 0; i< MAX_STACK && i <  Thread.currentThread().getStackTrace().length; i++)
    {
      StackTraceElement element = Thread.currentThread().getStackTrace()[i];
      sb.append(element.toString());
      sb.append("\n");
    }
  }

}