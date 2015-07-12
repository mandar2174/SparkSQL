package com.edu.dataframe;

public class TestP2 implements Test
{

	private int i2 = 0;

	public TestP2( int i2 )
	{
		super();
		this.i2 = i2;
	}

	public int getI2()
	{
		return i2;
	}

	public void setI2( int i2 )
	{
		this.i2 = i2;
	}

	@Override
	public void display()
	{
		System.out.println( "Value of i2 : " + getI2() );

	}

}
