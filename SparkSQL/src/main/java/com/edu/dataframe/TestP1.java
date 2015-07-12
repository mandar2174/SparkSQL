package com.edu.dataframe;

public class TestP1 implements Test
{
	private int i = 0;

	public TestP1( int i )
	{
		super();
		this.i = i;
	}

	public int getI()
	{
		return i;
	}

	public void setI( int i )
	{
		this.i = i;
	}

	@Override
	public void display()
	{
		System.out.println( "Value of the I :" + getI() );

	}

}
