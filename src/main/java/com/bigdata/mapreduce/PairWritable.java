/**
 * 
 */
package com.bigdata.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author haibozhang
 *
 */
public class PairWritable implements WritableComparable<PairWritable> {
	private String first;
	private int second;

	public PairWritable() {
		super();
	}
	
	public PairWritable(String first, int second) {
		super();
		this.set(first, second);
	}
	
	public void set(String first, int second){
		this.first = first;
		this.second = second;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}
	
	@Override
	public String toString() {
		return "PairWritable [first=" + first + ", second=" + second + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritable other = (PairWritable) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeInt(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readInt();
	}

	@Override
	public int compareTo(PairWritable o) {
		int cmp = this.getFirst().compareTo(o.getFirst());
		if(0 != cmp){
			return cmp;
		}
		return Integer.valueOf(this.getSecond()).compareTo(Integer.valueOf(o.getSecond()));
	}

}
