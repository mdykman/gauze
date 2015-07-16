package org.dykman.gauze.event;

import java.lang.annotation.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;


public class AnnotationClass {

    public AnnotationClass() {
	// TODO Auto-generated constructor stub
    }

    
    @NamedFuture("thing")
    public Future getAThing(Callable<Object> cc) {
	return new Task<Object>(cc);
    }
    
    
    static class Task<T> extends FutureTask<T> {
	public Task(Callable<T> callable) {
	    super(callable);
	}
    }
    
    
    
    
    
}
