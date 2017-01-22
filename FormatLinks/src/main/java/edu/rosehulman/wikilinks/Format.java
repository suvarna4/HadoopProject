package edu.rosehulman.wikilinks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Format extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		Object first = input.get(0);
		Object rest = input.get(1);
		long a = (Long) first;
		String c = (String) rest;
		String[] b = c.split(" ");
		List<Tuple> tuples = new ArrayList<Tuple>();
		for(int i = 1; i< b.length; i ++){
			long d = Long.parseLong(b[i]);
			Tuple tuple = TupleFactory.getInstance().newTuple();
			tuple.append(a);
			tuple.append(d);
			tuples.add(tuple);
		}
		DataBag bagOutput = BagFactory.getInstance().newDefaultBag(tuples);
		return bagOutput;
	}

}
