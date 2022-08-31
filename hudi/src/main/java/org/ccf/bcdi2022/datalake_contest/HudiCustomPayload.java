package org.ccf.bcdi2022.datalake_contest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

public class HudiCustomPayload implements HoodieRecordPayload<HudiCustomPayload> {
    private GenericRecord record;
    public HudiCustomPayload(GenericRecord record, Comparable orderingVal) {
        this.record = record;
    }


    @Override
    public HudiCustomPayload preCombine(HudiCustomPayload oldValue) {
        Long requests = Long.parseLong(this.record.get("requests").toString());
        long requests1 = requests + Long.parseLong(oldValue.record.get("requests").toString());
        this.record.put("requests", String.valueOf(requests1));
        return this;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

        Option<IndexedRecord> recordOption = getInsertValue(schema);
        if (!recordOption.isPresent()) {
            return Option.empty();
        }

        GenericRecord insertRecord = (GenericRecord) recordOption.get();
        GenericRecord currentRecord = (GenericRecord) currentValue;

        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<Schema.Field> fields = schema.getFields();
        fields.forEach(field -> {
            Object value = insertRecord.get(field.name());
            value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
            Object defaultValue = field.defaultVal();

            if (field.name().equals("requests")) {
//                        String ss = currentRecord.get(field.name()).toString();
//                        System.out.println(ss);
                Long aa = Long.parseLong(value.toString()) + Long.parseLong(currentRecord.get(field.name()).toString());
                builder.set(field, aa.toString());
            } else if (field.name().equals("name")) {
                if (value.toString().equals("null")) {
                    builder.set(field, currentRecord.get(field.name()).toString());
                }
            } else {
                builder.set(field, value);
            }
        });
        return Option.of(builder.build());
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
        return Option.of(this.record);
    }
}
