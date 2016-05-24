package hadoop.hbase;

/**
 * This class provides methods related to the operations on byte array.
 * 
 * @author SDU.lwlin
 */
public class ByteUtil
{
	/**
	 * 转换short为byte
	 * 
	 * @param b the byte array
	 * @param s
	 *              需要转换的short. 
	 * twk修改：修改大数放在byte的前面，使其具备排序意义
	 * @param index the position
	 * @return the start position of the next attribute
	 */
	public static int putShort(byte b[], short s, int index)
	{
		/*b[index + 1] = (byte) (s >> 8);
		putTinyInt(b, s % 256, index);
		*/
		b[index ] = (byte) (s >> 8);
		b[index+1] = (byte) (s % 256> 127 ? -(s % 256 - 127) : s % 256);
		return index + 2;
	}

	// public static int putShort(byte b[], short s, int index) {
	// b[index + 1] = (byte) (s >> 8);
	// b[index + 0] = (byte) (s >> 0);
	// return index + 2;
	// }

	/**
	 * 通过byte数组取到short
	 * 
	 * @param b the byte array
	 * @param index
	 *              第几位开始取
	 * twk修改：修改大数放在byte的前面，使其具备排序意义
	 * @return the corresponding short value
	 */
	public static short getShort(byte[] b, int index)
	{
		return (short) (((b[index ] << 8) | getTinyInt(b, index+1)));
	}

	// public static short getShort(byte[] b, int index)
	// {
	// return (short) (((b[index + 1] << 8) | b[index + 0] & 0xff));
	// }

	/**
	 * 转换int为byte数组
	 * 
	 * @param bb the byte array
	 * @param x the int value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putInt(byte[] bb, int x, int index)
	{
		bb[index + 3] = (byte) (x >> 24);
		bb[index + 2] = (byte) (x >> 16);
		bb[index + 1] = (byte) (x >> 8);
		bb[index + 0] = (byte) (x >> 0);
		return index + 4;
	}

	/**
	 * 通过byte数组取到int
	 * 
	 * @param bb the byte array
	 * @param index
	 *              第几位开始
	 * @return the corresponding int value
	 */
	public static int getInt(byte[] bb, int index)
	{
		return ((((bb[index + 3] & 0xff) << 24) | ((bb[index + 2] & 0xff) << 16) | ((bb[index + 1] & 0xff) << 8) | ((bb[index + 0] & 0xff) << 0)));
	}

	/**
	 * 转换long型为byte数组
	 * twk修改：修改大数放在byte的前面，使其具备排序意义
	 * @param bb the byte array
	 * @param x the long value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putLong(byte[] bb, long x, int index)
	{
		bb[index + 0] = (byte) (x >> 56);
		bb[index + 1] = (byte) (x >> 48);
		bb[index + 2] = (byte) (x >> 40);
		bb[index + 3] = (byte) (x >> 32);
		bb[index + 4] = (byte) (x >> 24);
		bb[index + 5] = (byte) (x >> 16);
		bb[index + 6] = (byte) (x >> 8);
		bb[index + 7] = (byte) (x >> 0);
		return index + 8;
	}

	/**
	 * 通过byte数组取到long
	 * 
	 * @param bb the byte array
	 * @param index the start position
	 * @return the corresponding long value
	 */
	public static long getLong(byte[] bb, int index)
	{
		return ((((long) bb[index + 0] & 0xff) << 56) | (((long) bb[index + 1] & 0xff) << 48)
		            | (((long) bb[index + 2] & 0xff) << 40) | (((long) bb[index + 3] & 0xff) << 32)
		            | (((long) bb[index + 4] & 0xff) << 24) | (((long) bb[index + 5] & 0xff) << 16)
		            | (((long) bb[index + 6] & 0xff) << 8) | (((long) bb[index + 7] & 0xff) << 0));
	}
	
	/**
	 * 字符到字节转换
	 * 
	 * @param bb the byte array
	 * @param ch the char value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putChar(byte[] bb, char ch, int index)
	{
		int temp = ch;
		// byte[] b = new byte[2];
		for (int i = 0; i < 2; i++)
		{
			bb[index + i] = new Integer(temp & 0xff).byteValue(); // 将最高位保存在最低位
			temp = temp >> 8; // 向右移8位
		}
		return index + 2;
	}

	/**
	 * 字节到字符转换
	 * 
	 * @param b the byte array
	 * @param index the start position
	 * @return the corresponding char value
	 */
	public static char getChar(byte[] b, int index)
	{
		int s = 0;
		if (b[index + 1] > 0)
			s += b[index + 1];
		else
			s += 256 + b[index + 0];
		s *= 256;
		if (b[index + 0] > 0)
			s += b[index + 1];
		else
			s += 256 + b[index + 0];
		char ch = (char) s;
		return ch;
	}

	/**
	 * float转换byte
	 * 
	 * @param bb the byte array
	 * @param x the float value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putFloat(byte[] bb, float x, int index)
	{
		// byte[] b = new byte[4];
		int l = Float.floatToIntBits(x);
		for (int i = 0; i < 4; i++)
		{
			bb[index + i] = new Integer(l).byteValue();
			l = l >> 8;
		}
		return index + 4;
	}

	/**
	 * 通过byte数组取得float
	 * 
	 * @param bb the byte array
	 * @param index the start position
	 * @return the corresponding float value
	 */
	public static float getFloat(byte[] b, int index)
	{
		int l;
		l = b[index + 0];
		l &= 0xff;
		l |= ((long) b[index + 1] << 8);
		l &= 0xffff;
		l |= ((long) b[index + 2] << 16);
		l &= 0xffffff;
		l |= ((long) b[index + 3] << 24);
		return Float.intBitsToFloat(l);
	}

	/**
	 * double转换byte
	 * 
	 * @param bb the byte array
	 * @param x the double value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putDouble(byte[] bb, double x, int index)
	{
		// byte[] b = new byte[8];
		long l = Double.doubleToLongBits(x);
		for (int i = 0; i < 4; i++)
		{
			bb[index + i] = new Long(l).byteValue();
			l = l >> 8;
		}
		return index + 8;
	}

	/**
	 * 通过byte数组取得float
	 * 
	 * @param bb the byte array
	 * @param index the start position
	 * @return the corresponding double value
	 */
	public static double getDouble(byte[] b, int index)
	{
		long l;
		l = b[0];
		l &= 0xff;
		l |= ((long) b[1] << 8);
		l &= 0xffff;
		l |= ((long) b[2] << 16);
		l &= 0xffffff;
		l |= ((long) b[3] << 24);
		l &= 0xffffffffl;
		l |= ((long) b[4] << 32);
		l &= 0xffffffffffl;
		l |= ((long) b[5] << 40);
		l &= 0xffffffffffffl;
		l |= ((long) b[6] << 48);
		l &= 0xffffffffffffffl;
		l |= ((long) b[7] << 56);
		return Double.longBitsToDouble(l);
	}

	/**
	 * Put a tiny int value into byte array.
	 * 
	 * @param bs the byte array
	 * @param i the tiny int value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putTinyInt(byte[] bs, int i, int index)
	{
		try
		{
			if (i < 0 || i > 255) { throw new Exception(
			            "TinyInt too huge: value expected >=0 and <= 255, but encountered=" + i); }
			bs[index] = (byte) (i > 127 ? -(i - 127) : i);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return index + 1;
	}

	/**
	 * Get a tiny int value from byte array.
	 * 
	 * @param bs the byte array
	 * @param index the start position
	 * @return the corresponding tiny int value
	 */
	public static int getTinyInt(byte[] bs, int index)
	{
		return (bs[index] < 0 ? 127 - bs[index] : bs[index]);
	}

	/**
	 * Put a string value into byte array.
	 * 
	 * @see #putString(byte[], String, String, int)
	 * @param bs the byte array
	 * @param str the string value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putString(byte[] bs, String str, int index)
	{
		return putString(bs, str, "UTF-8", index);
	}

	/**
	 * Put a string value into byte array encoded by some way.
	 * 
	 * @see #putString(byte[], String, int)
	 * @param bs the byte array
	 * @param str the string value
	 * @param encoding the encoding way
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putString(byte[] bs, String str, String encoding, int index)
	{
		if (str == null)
		{
			bs[index] = 0;
			return index + 1;
		}
		else
		{
			byte[] bsStr = null;
			try
			{
				bsStr = str.getBytes(encoding);
				if (bsStr.length > 255)
				{
					throw new Exception("String too long: length expected<256, but encountered="
					            + bsStr.length);
				}
				else
				{
					index = putTinyInt(bs, bsStr.length, index);
					System.arraycopy(bsStr, 0, bs, index, bsStr.length);
					return index + bsStr.length;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				bs[index] = 0;
				return index = 1;
			}
		}
	}

	/**
	 * 只为CarPlateNumber定制使用，是修复了一个bug，详情咨询SDU.lwlin, SDU.zhdong
	 * 
	 * @param bs the byte array
	 * @param str the string value
	 * @param index the start position
	 * @return the start position of the next attribute
	 */
	public static int putFixedString(byte[] bs, String str, int index)
	{
		final int LEN = 10;
		byte[] empty = new byte[LEN];
		int finalIndex = index + LEN;
		System.arraycopy(empty, 0, bs, index, LEN);
		if (str == null)
		{
			return finalIndex;
		}
		else
		{
			byte[] bsStr = null;
			try
			{
				bsStr = str.getBytes("GBK");
				if (bsStr.length > LEN)
				{
					throw new Exception("String too long: length expected<= " + LEN + ", but encountered="
					            + bsStr.length);
				}
				else
				{
					System.arraycopy(bsStr, 0, bs, index, bsStr.length);
					return finalIndex;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
				return finalIndex;
			}
		}
	}

	/**
	 * Get a string value from byte array.
	 * 
	 * @see #getString(byte[], String, int)
	 * @param bs the byte array
	 * @param index the start position
	 * @return the corresponding string value
	 */
	public static String getString(byte[] bs, int index)
	{
		String str = getString(bs, "UTF-8", index);
		return str;
	}

	/**
	 * Get a string value from byte array by some encoding way.
	 * 
	 * @see #getString(byte[], int)
	 * @param bs  the byte array
	 * @param encoding the encoding way
	 * @param index the start position
	 * @return the corresponding string value
	 */
	public static String getString(byte[] bs, String encoding, int index)
	{
		int length = getTinyInt(bs, index);
		if (length < 0)
			return null;
		else
		{
			String str = null;
			try
			{
				str = new String(bs, index + 1, length, encoding);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			return str;
		}
	}

	/**
	 * 只为CarPlateNumber定制使用，是修复了一个bug，详情咨询SDU.lwlin, SDU.zhdong
	 * 
	 * @param bs the byte array
	 * @param index the start position
	 * @return the corresponding string value
	 */
	public static String getFixedString(byte[] bs, int index)
	{
		final int LEN = 10;
		String str = null;
		try
		{
			str = new String(bs, index, LEN, "GBK").trim();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return str;
	}

	/**
	 * Get the length of a string value represented as bytes.
	 * 
	 * @see #calBytesLengthForString(String, String)
	 * @param str the string value
	 * @return the corresponding length
	 */
	public static int calBytesLengthForString(String str)
	{
		return calBytesLengthForString(str, "UTF-8");
	}

	/**
	 * Get the length of a string value represented as bytes in some encoding way.
	 * 
	 * @see #calBytesLengthForString(String)
	 * @param str the string value
	 * @param encoding the encoding way
	 * @return the corresponding length
	 */
	public static int calBytesLengthForString(String str, String encoding)
	{
		if (str == null)
			return 1;
		else
		{
			byte[] bs = null;
			try
			{
				bs = str.getBytes(encoding);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			return bs == null ? Integer.MIN_VALUE : bs.length + 1;
		}
	}

	/**
	 * Transform a long value into byte array.
	 * 
	 * @param value the long value
	 * @return the corresponding byte array
	 */
	public static byte[] getLongBytes(long value)
	{
		byte[] b = new byte[8];
		putLong(b, value, 0);
		return b;
	}

	public static void main(String[] args)
	{
		System.out.println(getShort(new byte[] { -49, 0 }, 0));
	}
}