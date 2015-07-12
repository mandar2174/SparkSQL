package com.edu.dataframe;

public class TestMain
{

	public static void main( String[] args )
	{
		Test t1 = new TestP1( 10 );
		t1.display();

		Test t2 = new TestP2( 13 );
		t2.display();
		
		System.out.println(((TestP1)t1).getI());

	}
}
