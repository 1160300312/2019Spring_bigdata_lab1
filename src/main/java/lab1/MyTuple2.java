package lab1;

import scala.Tuple2;

public class MyTuple2<T1,T2> extends Tuple2<T1,T2>{

	private static final long serialVersionUID = 1L;

	public MyTuple2(T1 _1, T2 _2) {
		super(_1, _2);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public boolean equals(Object o){
		if(o == null)
			return false;
		if(this == o)
			return true;
		if(!(o instanceof MyTuple2)){
			System.out.println(1);
			return false;
		} 
		MyTuple2<?,?> my = (MyTuple2<?,?>)o;
		System.out.println(this._1);
		System.out.println(my._1);
		return this._1.equals(my._1)&&this._2.equals(my._2);
	}
}
