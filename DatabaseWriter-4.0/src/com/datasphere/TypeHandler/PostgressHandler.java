package com.datasphere.TypeHandler;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.math.NumberUtils;

public class PostgressHandler extends TargetTypeHandler {
	@Override
	public void initialize() {
		super.initialize();
		this.addToMap(new StringToNumberHandler());
		this.addToMap(new StringToIntegerHandler());
		this.addToMap(new StringToBigIntHandler());
		this.addToMap(new StringToDoubleHandler());
		this.addToMap(new StringToRealHandler());
		this.addToMap(new StringToSmallIntHandler());
		this.addToMap(new StringToTimestampHandler());
	}

	class StringToNumberHandler extends TypeHandler {
		public StringToNumberHandler() {
			super(2, String.class);
		}

		public StringToNumberHandler(final int sqlType) {
			super(sqlType, String.class);
		}

		@Override
		public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
			if (value == null || ((String) value).trim().length() == 0) {
				stmt.setNull(bindIndex, java.sql.Types.BIGINT);
			} else {
				final BigDecimal bd = new BigDecimal((String) value);
				stmt.setBigDecimal(bindIndex, bd);
			}
		}
	}

	class StringToIntegerHandler extends StringToNumberHandler {
		public StringToIntegerHandler() {
			super(4);
		}
	}

	class StringToFloatHandler extends StringToNumberHandler {
		public StringToFloatHandler() {
			super(6);
		}
	}

	class StringToDoubleHandler extends StringToNumberHandler {
		public StringToDoubleHandler() {
			super(8);
		}
	}

	class StringToBigIntHandler extends StringToNumberHandler {
		public StringToBigIntHandler() {
			super(-5);
		}
	}

	class StringToRealHandler extends StringToNumberHandler {
		public StringToRealHandler() {
			super(7);
		}
	}

	class StringToSmallIntHandler extends StringToNumberHandler {
		public StringToSmallIntHandler() {
			super(5);
		}
	}

	class StringToTimestampHandler extends TypeHandler {
		public StringToTimestampHandler() {
			super(93, String.class);
		}

		public StringToTimestampHandler(final int sqlType) {
			super(sqlType, String.class);
		}

		@Override
		public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
			try {
				if (value == null || ((String) value).trim().length() == 0) {
					stmt.setNull(bindIndex, java.sql.Types.TIMESTAMP);
				} else {
					final Timestamp timeStamp = Timestamp.valueOf((String) value);
					stmt.setTimestamp(bindIndex, timeStamp);
				}
			} catch (Exception e) {
				String v = (String) value;

				if (NumberUtils.isNumber(v)) {
					Timestamp timeStamp = new Timestamp(new Date(Long.parseLong(v)).getTime());
					stmt.setTimestamp(bindIndex, timeStamp);
				} else {
					SimpleDateFormat formatter = null;
					Timestamp timeStamp = null;
					try {
						if (v.indexOf("/") > -1) {
							v =v.replace("/", "-");
						}
						boolean isOtherFormatter = false;
						String[] dt = v.split("-");
						if (dt.length == 3) {
							if(v.length() > 19) {
								v = v.substring(0,19);
							}
							if(v.length() == 19) {
								if (dt[0].length() == 2) {
									formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
									timeStamp = new Timestamp(formatter.parse(v).getTime());
									stmt.setTimestamp(bindIndex, timeStamp);
								} else if (dt[0].length() == 4) {
									formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									timeStamp = new Timestamp(formatter.parse(v).getTime());
									stmt.setTimestamp(bindIndex, timeStamp);
								} else {
									isOtherFormatter = true;
								}
							}else if(v.length() == 10){
								if (dt[0].length() == 2) {
									formatter = new SimpleDateFormat("dd-MM-yyyy");
									timeStamp = new Timestamp(formatter.parse(v).getTime());
									stmt.setTimestamp(bindIndex, timeStamp);
								} else if (dt[0].length() == 4) {
									formatter = new SimpleDateFormat("yyyy-MM-dd");
									timeStamp = new Timestamp(formatter.parse(v).getTime());
									stmt.setTimestamp(bindIndex, timeStamp);
								} else {
									isOtherFormatter = true;
								}
							}else {
								isOtherFormatter = true;
							}
						}else {
							isOtherFormatter = true;
						}
						if(isOtherFormatter) {
							System.out.println("-----未知的日期格式,此字段已赋空值：" + v);
							stmt.setNull(bindIndex, java.sql.Types.TIMESTAMP);
						}
					} catch (ParseException e1) {
						stmt.setNull(bindIndex, java.sql.Types.TIMESTAMP);
					}

				}
			}
		}
	}
}
