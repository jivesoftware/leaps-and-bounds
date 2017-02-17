package com.jivesoftware.os.lab.io;

/**
 * @author jonathan.colt
 */
public class BolBuffers {


    private int bolBuffersStackDepth = 0;
    private BolBuffer[] bolBuffers;

    public BolBuffers(int capacity) {
        this.bolBuffers = new BolBuffer[capacity];
    }

    public BolBuffer allocate() {
        BolBuffer bolBuffer;
        if (bolBuffersStackDepth > 0 && bolBuffers[bolBuffersStackDepth - 1] != null) {
            bolBuffersStackDepth--;
            bolBuffer = bolBuffers[bolBuffersStackDepth];
            bolBuffers[bolBuffersStackDepth] = null;
        } else {
            bolBuffer = new BolBuffer();
        }
        return bolBuffer;
    }

    public void recycle(BolBuffer bolBuffer) {
        if (bolBuffer != null && bolBuffersStackDepth < bolBuffers.length) {
            bolBuffers[bolBuffersStackDepth] = bolBuffer;
            bolBuffersStackDepth++;
        }
    }

}
