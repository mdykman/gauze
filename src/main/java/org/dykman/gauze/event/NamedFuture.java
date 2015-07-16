package org.dykman.gauze.event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.lang.reflect.Method;


@Target({ElementType.METHOD,ElementType.PARAMETER})
public @interface NamedFuture {
    String value();
    
}
