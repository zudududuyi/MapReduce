package com.YTao.mapreduce.Big_Project.aaNotImportant.uselessCode.Top100Scores;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements WritableComparable<IntPairWritable> {

    private IntWritable first;
    private IntWritable second;

    public IntPairWritable() {
        this.first = new IntWritable();
        this.second = new IntWritable();
    }

    public IntPairWritable(int first, int second) {
        this.first = new IntWritable(first);
        this.second = new IntWritable(second);
    }

    public int getFirst() {
        return first.get();
    }

    public int getSecond() {
        return second.get();
    }

    public void set(int first, int second) {
        this.first.set(first);
        this.second.set(second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int compareTo(IntPairWritable other) {
        int cmp = first.compareTo(other.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(other.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntPairWritable) {
            IntPairWritable other = (IntPairWritable) obj;
            return first.equals(other.first) && second.equals(other.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}