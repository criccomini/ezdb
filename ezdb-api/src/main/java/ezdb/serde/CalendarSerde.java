package ezdb.serde;

import java.util.Calendar;
import java.util.Date;

public class CalendarSerde implements Serde<Calendar> {

    public static final CalendarSerde get = new CalendarSerde();

    @Override
    public Calendar fromBytes(final byte[] bytes) {
        final Date date = DateSerde.get.fromBytes(bytes);
        if (date == null) {
            return null;
        } else {
            final Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            return cal;
        }
    }

    @Override
    public byte[] toBytes(final Calendar obj) {
        Date date;
        if (obj == null) {
            date = null;
        } else {
            date = obj.getTime();
        }
        return DateSerde.get.toBytes(date);
    }

}
