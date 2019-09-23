package com.datasphere.tungsten;

public class ReportCommandHelper
{
    public static void showUsage() {
        System.out.println("Report command usages are as follows : ");
        System.out.println("1. report start <app_name>");
        System.out.println("Ex : report start posapp");
        System.out.println("2. report stop <app_name>");
        System.out.println("Ex : report stop posapp");
        System.out.println("3. report latency <app_name>");
        System.out.println("Ex : report latency posapp");
        System.out.println("4. report latency <app_name> all");
        System.out.println("Ex : report latency posapp all");
    }
}
