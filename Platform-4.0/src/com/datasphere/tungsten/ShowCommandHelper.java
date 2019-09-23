package com.datasphere.tungsten;

public class ShowCommandHelper
{
    public static void showUsage() {
        System.out.println("Show command usage is as follows");
        System.out.println("1. show <stream_name>");
        System.out.println("Ex : show filterStream");
        System.out.println("2. show <src/targetName> LINEAGE");
        System.out.println("Ex : show fileSource LINEAGE");
        System.out.println("3. show <src/targetName> LINEAGE LIMIT <count>");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10");
        System.out.println("4. show <src/targetName> LINEAGE LIMIT <count> <list_of_params>");
        System.out.println("List of params can be the following");
        System.out.println("a. -start '2017-09-23'");
        System.out.println("b. -end '2017-09-23'");
        System.out.println("c. -status 'CREATED'");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10 -start '2017-09-23'");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10 -end '2017-09-23'");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10 -status 'CREATED'");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10 -start '2017-09-23' -end '2017-09-24' -status 'CREATED'");
        System.out.println("5. show <src/targetName> LINEAGE LIMIT <count> <list_of_params> <ASC/DESC>");
        System.out.println("Ex : show fileSource LINEAGE LIMIT 10 -start '2017-09-23' -end '2017-09-24' -status 'CREATED' ASC");
    }
}
