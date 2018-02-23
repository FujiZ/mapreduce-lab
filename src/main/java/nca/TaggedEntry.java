package nca;

import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedEntry extends TaggedMapOutput {
    private Class<? extends Writable> dataClass;
    private Text key;
    private Writable data;

    public TaggedEntry(Class<? extends Writable> dataClass) {
        if (dataClass == null) {
            throw new IllegalArgumentException("null dataClass");
        } else {
            this.dataClass = dataClass;
        }
        this.key = new Text();
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public Writable getData() {
        return data;
    }

    public void setData(Writable data) {
        this.data = data;
    }

    public void write(DataOutput out) throws IOException {
        this.tag.write(out);
        this.key.write(out);
        this.data.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.tag.readFields(in);
        this.key.readFields(in);
        this.data = WritableFactories.newInstance(this.dataClass);
        this.data.readFields(in);
    }
}
