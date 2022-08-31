package org.ccf.bcdi2022.datalake_contest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

public class HudiCustomPayload extends OverwriteWithLatestAvroPayload{
    public HudiCustomPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public HudiCustomPayload(Option<GenericRecord> record) {
        super(record); // natural order
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

        Option<IndexedRecord> recordOption = getInsertValue(schema);
        if (!recordOption.isPresent()) {
            return Option.empty();
        }

        GenericRecord insertRecord = (GenericRecord) recordOption.get();
        GenericRecord currentRecord = (GenericRecord) currentValue;

        if (isDeleteRecord(insertRecord)) {
            return Option.empty();
        } else {
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            List<Schema.Field> fields = schema.getFields();
            fields.forEach(field -> {
                Object value = insertRecord.get(field.name());
                value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
                Object defaultValue = field.defaultVal();
                if (!overwriteField(value, defaultValue)) {
                    if (field.name().equals("requests")) {
//                        String ss = currentRecord.get(field.name()).toString();
//                        System.out.println(ss);
                        builder.set(field, Long.parseLong(value.toString()) + Long.parseLong(currentRecord.get(field.name()).toString()));
                    } else if (field.name().equals("name")) {
                        if (value.toString().equals("null")) {
                            builder.set(field, currentRecord.get(field.name()).toString());
                        }
                    } else {
                        builder.set(field, value);
                    }
                } else {
                    builder.set(field, currentRecord.get(field.pos()));
                }
            });
            return Option.of(builder.build());
        }
    }
}
