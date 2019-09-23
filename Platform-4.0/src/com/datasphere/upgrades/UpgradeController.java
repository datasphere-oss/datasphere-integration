package com.datasphere.upgrades;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import org.reflections.Reflections;

import java_cup.runtime.Scanner;

public class UpgradeController
{
    public static void upgrade(final String from, final String to) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        final Reflections reflections = new Reflections("com.datasphere.upgrades", new Scanner[0]);
        final Set<Class<? extends Upgrade>> subTypes = (Set<Class<? extends Upgrade>>)reflections.getSubTypesOf((Class)Upgrade.class);
        for (final Class c : subTypes) {
            final Method method = c.getMethod("upgrade", String.class, String.class);
            method.invoke(c.newInstance(), from, to);
        }
    }
}
