package org.dykman.gauze.event;

import java.util.LinkedList;

public class Gasket //implements Steward 
{

    Gasket steward = null;
    LinkedList<Gasket> waiting = new LinkedList<>();
    LinkedList<Gasket> running = new LinkedList<>();
    String label;
    /*
    public Gasket(Gasket steward) {
	this.steward = steward;
    }
    */
    public Gasket(String label) {
	this.label = label;
    }
    public void addGasket(Gasket gasket) {
	gasket.setSteward(gasket);
	synchronized (this) {
	    waiting.add(gasket);
	}
    }

    public void run() {
	System.out.println("running gasket " + label);
	if(steward != null)  {
	    steward.onComplete(this);
	}
	
	onComplete();
    }

    public void setSteward(Gasket parent) {
	steward = parent;
    }
    public void onComplete(Gasket child) {
	synchronized(this) {
	    running.remove(child);
	}
    }    
    
    public void onComplete() {
	synchronized (this) {
	    if(running.size() + waiting.size() == 0) {
		if(steward != null) steward.onComplete();
	    } else if(waiting.size() > 0) {
        	Gasket child = waiting.remove();
        	running.add(child);
        	child.run();
	    }
	}
    }
    
    public static void main(String[] args) {
	Gasket parent = new Gasket("parent");
	Gasket runner = new Gasket("runner");
	parent.addGasket(runner);

	Gasket task1 = new Gasket("task1");
	Gasket task2 = new Gasket("task2");
	runner.addGasket(task1);
	runner.addGasket(task2);
	parent.run();
    }
    
}
