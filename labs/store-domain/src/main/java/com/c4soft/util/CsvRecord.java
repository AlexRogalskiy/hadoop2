package com.c4soft.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>/!\ Do not use for production purpose /!\</h1> Helper class for CSV
 * records (i.e. CSV lines).<br>
 * Unquoted empty values are interpreted as null values.<br>
 * Non double-quoted values are trimmed.<br>
 * Double quotes are not allowed in fields values (no possible escaping) =>
 * results in hazardous CSV line interpretation or Runtime exception when
 * constructing from fields.
 * 
 * @author ch4mp
 */
public class CsvRecord {
    /**
     * Capture pattern for any field but the last one
     */
    public static final Pattern FIELDS_REGEX = Pattern.compile("(?:(?:\\s*\"([^\"]*)\"[^,\"]*)|([^,\"]*)),");

    /**
     * Capture pattern for the last field
     */
    public static final Pattern LAST_FIELD_REGEX = Pattern.compile(",?(?:(?:\\s*\"([^\"]*)\"[^,\"]*)|([^,\"]*))$");

    private final List<String> fields;

    /**
     * Splits a CSV line into records and constructs a record out of it
     * 
     * @param line
     *            a CSV line
     */
    public CsvRecord(String line) {
        Matcher fieldsMatcher = FIELDS_REGEX.matcher(line);

        fields = new ArrayList<String>();

        while (fieldsMatcher.find()) {
            fields.add(extractField(fieldsMatcher));
        }

        Matcher lastFieldMatcher = LAST_FIELD_REGEX.matcher(line);
        if (lastFieldMatcher.find()) {
            fields.add(extractField(lastFieldMatcher));
        }
    }

    /**
     * Constructs a record out of it's values
     * 
     * @param someFields
     *            array containing fields values
     * @throws NotImplementedException
     *             if a field contains double quotes
     */
    public CsvRecord(String[] someFields) {
        this.fields = new ArrayList<String>(someFields.length);
        for (String field : someFields) {
            if (field.contains("\"")) {
                throw new NotImplementedException("CsvRecord does not currently allow quotes in fields: " + field);
            }
            this.fields.add(field);
        }
    }

    /**
     * Constructs a record out of it's values
     * 
     * @param someFields
     *            collection containing fields values
     * @throws NotImplementedException
     *             if a field contains double quotes
     */
    public CsvRecord(Collection<String> someFields) {
        this(someFields.toArray(new String[someFields.size()]));
    }

    /**
     * Returns the field at the specified position in this record.
     * 
     * @param idx
     *            index of the element to return
     * @return the field at the specified position in this record
     * @throws IndexOutOfBoundsException
     *             - if the index is out of range (idx < 0 || idx >= size())
     */
    public String get(int idx) {
        return fields.get(idx);
    }

    /**
     * Returns the number of fields in this record.
     * 
     * @return the number of fields in this record
     */
    public int size() {
        return fields.size();
    }

    /**
     * Turns a collection of fields into a CSV line
     * 
     * @return CSV line (quoted fields with comma separator)
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean isFirst = true;
        for (String field : fields) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(',');
            }
            if (field != null) {
                sb.append('"');
                sb.append(field);
                sb.append('"');
            }
        }

        return sb.toString();
    }

    private static String extractField(Matcher m) {
        String quotedField = m.group(1);
        if (quotedField != null) {
            return quotedField;
        }
        String unquotedField = m.group(2);
        if (unquotedField == null || unquotedField.length() == 0) {
            return null;
        }
        return unquotedField.trim();
    }

    public static final class NotImplementedException extends RuntimeException {
        private static final long serialVersionUID = 4326901378545053667L;

        public NotImplementedException(String msg) {
            super(msg);
        }
    }
}
