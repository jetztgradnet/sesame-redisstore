package net.jetztgrad.sesame.redis.util;

public class KeyBuilder {
	public final static char DEFAULT_KEYSEPARATOR = ':';
	public final static char DEFAULT_VALUESEPARATOR = '|';
	protected final String keyPrefix;
	protected final char keySeparator;
	protected final char valueSeparator;
	protected final boolean trimKeys;
	protected final StringBuilder builder;

	public KeyBuilder() {
		this(null, DEFAULT_KEYSEPARATOR, DEFAULT_VALUESEPARATOR);
	}
	
	public KeyBuilder(final String prefix) {
		this(prefix, DEFAULT_KEYSEPARATOR, DEFAULT_VALUESEPARATOR);
	}
	
	public KeyBuilder(KeyBuilder parent) {
		this(parent.getKey(), parent.getKeySeparator(), parent.getValueSeparator(), parent.isTrimKeys());
	}

	public KeyBuilder(final char keySeparator, final char valueSeparator) {
		this(null, keySeparator, valueSeparator);
	}

	public KeyBuilder(final String prefix, final char keySeparator, final char valueSeparator) {
		this(prefix, keySeparator, valueSeparator, true);
	}
	
	public KeyBuilder(final String prefix, final char keySeparator, final char valueSeparator, boolean trimKeys) {
		this.keyPrefix = prefix;
		this.keySeparator = keySeparator;
		this.valueSeparator = valueSeparator;
		this.builder = new StringBuilder();
		this.trimKeys = trimKeys;
	}
	
	public boolean isTrimKeys() {
		return trimKeys;
	}
	
	public String getPrefix() {
		return keyPrefix;
	}
	
	public char getKeySeparator() {
		return keySeparator;
	}
	public char getValueSeparator() {
		return valueSeparator;
	}
	
	/**
	 * Append key. Shortcut for {@link #appendKey(String)}.
	 * 
	 * @param key key to append.
	 * 
	 * @return this KeyBuilder instanced for chained use
	 */
	public KeyBuilder k(String key) {
		return appendKey(key);
	}
	
	/**
	 * Append key. 
	 * 
	 * The key is separated from previous parts by {@link #keySeparator}.
	 * 
	 * @param key key to append.
	 * 
	 * @return this KeyBuilder instanced for chained use
	 */
	public KeyBuilder appendKey(String key) {
		return append(key, keySeparator);
	}
	
	/**
	 * Append value. Shortcut for {@link #appendValue(String)}.
	 * 
	 * @param value value to append.
	 * 
	 * @return this KeyBuilder instanced for chained use
	 */
	public KeyBuilder v(String value) {
		return appendValue(value);
	}

	/**
	 * Append value. 
	 * 
	 * The value is separated from previous parts by {@link #valueSeparator}. 
	 * 
	 * @param value value to append.
	 * 
	 * @return this KeyBuilder instanced for chained use
	 */
	public KeyBuilder appendValue(String value) {
		return append(value, valueSeparator);
	}

	/**
	 * Append text. 
	 * 
	 * The text is separated from previous parts by the specified separator. 
	 * 
	 * @param text text to append.
	 * @param separator separator to use before the text
	 * 
	 * @return this KeyBuilder instanced for chained use
	 */
	public KeyBuilder append(String text, char separator) {
		if (text != null) {
			if (trimKeys) {
				text = text.trim();
			}
			// append separator
			if (builder.length() > 0) {
				builder.append(separator);
			}
			builder.append(text);
		}
		return this;
	}
	
	/**
	 * Get complete key.
	 * 
	 * @return key
	 */
	public String getKey() {
		String key = builder.toString();
		
		//builder.x();
		
		return key;
	}
	
	/**
	 * Reset contents to empty string.
	 */
	public void reset() {
		builder.setLength(0);
	}
	
	@Override
	public String toString() {
		return getKey();
	}
}
