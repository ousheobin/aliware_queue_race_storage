/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.utils;

public class LogUtil {

    public static void warn(String message){
        System.out.println("[WARNING\t] ["+Thread.currentThread().getName()+"\t] "+message);
    }

    public static void info(String message){
        System.out.println("[INFO\t] ["+Thread.currentThread().getName()+"\t] "+message);
    }

    public static void info(Object message){
        System.out.println("[INFO\t] ["+Thread.currentThread().getName()+"\t] "+message.toString());
    }

    public static void error(String message){
        System.out.println("[ERROR\t] ["+Thread.currentThread().getName()+"\t] "+message);
    }

    public static void error(String message,Throwable throwable){
        System.out.println("[ERROR\t] ["+Thread.currentThread().getName()+"\t] "+message);
        throwable.printStackTrace(System.out);
    }
}
